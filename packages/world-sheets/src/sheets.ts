import type { AccessTokenProvider } from './auth.js';
import { TAB_COLUMNS, type TabName, tabName } from './schema.js';

const API = 'https://sheets.googleapis.com/v4/spreadsheets';

interface SheetInfo {
  sheetId: number;
  title: string;
}

/**
 * Thin Sheets v4 client. Only the operations world-sheets actually uses —
 * keeps the dep surface minimal (no googleapis SDK; just fetch + the auth
 * provider). Every operation goes through one `request()` helper that adds
 * the bearer token, JSON-encodes the body, and parses errors.
 */
export class SheetsClient {
  private existingTabs: Map<string, SheetInfo> | null = null;

  constructor(
    private readonly spreadsheetId: string,
    private readonly getToken: AccessTokenProvider,
    private readonly tabPrefix: string
  ) {}

  private async request<T>(
    path: string,
    init: RequestInit = {}
  ): Promise<T> {
    const token = await this.getToken();
    const res = await fetch(`${API}/${this.spreadsheetId}${path}`, {
      ...init,
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
        ...init.headers,
      },
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(
        `[world-sheets] ${init.method ?? 'GET'} ${path} failed: HTTP ${res.status} ${text.slice(0, 400)}`
      );
    }
    return (await res.json()) as T;
  }

  async loadSheetMetadata(): Promise<void> {
    const body = await this.request<{
      sheets: Array<{ properties: { sheetId: number; title: string } }>;
    }>('?fields=sheets.properties');
    this.existingTabs = new Map(
      body.sheets.map((s) => [s.properties.title, s.properties])
    );
  }

  private tabFor(tab: TabName): string {
    return tabName(this.tabPrefix, tab);
  }

  /** Create the tab + write headers if missing. Idempotent. */
  async ensureSheet(tab: TabName): Promise<void> {
    if (!this.existingTabs) await this.loadSheetMetadata();
    const title = this.tabFor(tab);
    if (this.existingTabs!.has(title)) return;

    await this.request('/:batchUpdate', {
      method: 'POST',
      body: JSON.stringify({
        requests: [{ addSheet: { properties: { title } } }],
      }),
    });

    // Write the header row.
    const headers = TAB_COLUMNS[tab];
    await this.request(
      `/values/${encodeURIComponent(title)}!A1:append?valueInputOption=RAW`,
      {
        method: 'POST',
        body: JSON.stringify({ values: [headers] }),
      }
    );

    // Refresh metadata so we know the new sheetId.
    await this.loadSheetMetadata();
  }

  /** Append rows to a tab. Each `row` is one record (array of cell values). */
  async appendRows(tab: TabName, rows: string[][]): Promise<void> {
    if (rows.length === 0) return;
    const title = this.tabFor(tab);
    await this.request(
      `/values/${encodeURIComponent(title)}!A1:append?valueInputOption=RAW&insertDataOption=INSERT_ROWS`,
      {
        method: 'POST',
        body: JSON.stringify({ values: rows }),
      }
    );
  }

  /**
   * Read all data rows (skip the header row). Returns an array of cell
   * arrays — empty trailing cells are dropped by Sheets but the caller's
   * column count is fixed.
   */
  async getAllRows(tab: TabName): Promise<string[][]> {
    const title = this.tabFor(tab);
    const body = await this.request<{ values?: string[][] }>(
      `/values/${encodeURIComponent(title)}!A2:Z`
    );
    return body.values ?? [];
  }

  /**
   * Overwrite a specific row (1-based, including header — so the first data
   * row is index 2). Pads short rows with empty strings so column ordering
   * is preserved.
   */
  async updateRow(
    tab: TabName,
    rowNumber: number,
    values: string[]
  ): Promise<void> {
    const cols = TAB_COLUMNS[tab];
    const padded = [...values];
    while (padded.length < cols.length) padded.push('');
    const title = this.tabFor(tab);
    const lastCol = String.fromCharCode(64 + cols.length); // A..Z (we stay under 26 cols)
    await this.request(
      `/values/${encodeURIComponent(title)}!A${rowNumber}:${lastCol}${rowNumber}?valueInputOption=RAW`,
      {
        method: 'PUT',
        body: JSON.stringify({ values: [padded] }),
      }
    );
  }

  /**
   * Delete a data row by its 1-based row number. Uses the sheets-level
   * batchUpdate so the rows below shift up.
   */
  async deleteRow(tab: TabName, rowNumber: number): Promise<void> {
    if (!this.existingTabs) await this.loadSheetMetadata();
    const title = this.tabFor(tab);
    const info = this.existingTabs!.get(title);
    if (!info) return;
    await this.request('/:batchUpdate', {
      method: 'POST',
      body: JSON.stringify({
        requests: [
          {
            deleteDimension: {
              range: {
                sheetId: info.sheetId,
                dimension: 'ROWS',
                startIndex: rowNumber - 1,
                endIndex: rowNumber,
              },
            },
          },
        ],
      }),
    });
  }
}
