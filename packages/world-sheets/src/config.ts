import {
  type AccessTokenProvider,
  makeServiceAccountTokenProvider,
  parseServiceAccountKey,
  type ServiceAccountKey,
} from './auth.js';

export interface SheetsWorldConfig {
  /**
   * Google Sheets spreadsheet ID. The world creates / uses tabs inside this
   * sheet. Resolved from `GOOGLE_SHEETS_SPREADSHEET_ID` if not passed.
   */
  spreadsheetId?: string;

  /**
   * Service-account JSON. Accepted as: object, raw JSON string, or base64(JSON).
   * Resolved from `GOOGLE_SERVICE_ACCOUNT_JSON` env var if not passed.
   * Mutually exclusive with `accessTokenProvider`.
   */
  serviceAccountKey?: ServiceAccountKey | string;

  /**
   * Custom access-token provider. Use this if you'd rather wire your own
   * auth (ADC / Workload Identity Federation / OAuth). Mutually exclusive
   * with `serviceAccountKey`.
   */
  accessTokenProvider?: AccessTokenProvider;

  /** Tab-name prefix. Default empty. Lets multiple apps share a spreadsheet. */
  tabPrefix?: string;

  /** Reported by getDeploymentId. Default `dpl_sheets_local`. */
  deploymentId?: string;

  /**
   * Where the workflow runtime is hosted publicly — the dispatcher POSTs to
   * `{baseUrl}/.well-known/workflow/v1/{flow|step}?msg=…`. Resolved from
   * `WORKFLOW_BASE_URL` if not passed.
   */
  baseUrl?: string;

  /**
   * Start the in-process dispatcher pump on `start()`. Default true. Set false
   * on read-only consumers (dashboards).
   */
  startDispatcher?: boolean;

  /** Dispatcher poll interval (ms). Sheets is rate-limited; default 2000. */
  dispatcherPollMs?: number;

  /** Max delivery attempts before a job is dropped. Default 10. */
  maxAttempts?: number;
  /** Base backoff (ms) for redelivery after a failed dispatch. Default 5000. */
  retryBaseMs?: number;

  /** Re-enqueue pending/running runs on start. Default true. */
  recoverActiveRuns?: boolean;

  /**
   * Bootstrap missing tabs + headers on first use. Default true. Set false if
   * you maintain the sheet schema yourself.
   */
  autoBootstrap?: boolean;
}

export interface ResolvedSheetsConfig {
  spreadsheetId: string;
  accessTokenProvider: AccessTokenProvider;
  tabPrefix: string;
  deploymentId: string;
  baseUrl: string;
  startDispatcher: boolean;
  dispatcherPollMs: number;
  maxAttempts: number;
  retryBaseMs: number;
  recoverActiveRuns: boolean;
  autoBootstrap: boolean;
}

function envBaseUrl(): string {
  if (process.env.WORKFLOW_BASE_URL) return process.env.WORKFLOW_BASE_URL;
  if (process.env.APP_BASE_URL) return process.env.APP_BASE_URL;
  return `http://localhost:${process.env.PORT ?? 3000}`;
}

function numEnv(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}

export function resolveSheetsConfig(
  partial: SheetsWorldConfig = {}
): ResolvedSheetsConfig {
  const env = process.env;

  const spreadsheetId =
    partial.spreadsheetId ?? env.GOOGLE_SHEETS_SPREADSHEET_ID;
  if (!spreadsheetId) {
    throw new Error(
      '[world-sheets] No spreadsheet configured. Set GOOGLE_SHEETS_SPREADSHEET_ID env var or pass { spreadsheetId } to createWorld().'
    );
  }

  let accessTokenProvider: AccessTokenProvider;
  if (partial.accessTokenProvider) {
    accessTokenProvider = partial.accessTokenProvider;
  } else {
    const keySource = partial.serviceAccountKey ?? env.GOOGLE_SERVICE_ACCOUNT_JSON;
    if (!keySource) {
      throw new Error(
        '[world-sheets] No auth configured. Set GOOGLE_SERVICE_ACCOUNT_JSON env var (JSON or base64(JSON)) or pass { serviceAccountKey } / { accessTokenProvider } to createWorld().'
      );
    }
    accessTokenProvider = makeServiceAccountTokenProvider(
      parseServiceAccountKey(keySource)
    );
  }

  return {
    spreadsheetId,
    accessTokenProvider,
    tabPrefix: partial.tabPrefix ?? env.WORKFLOW_SHEETS_TAB_PREFIX ?? '',
    deploymentId:
      partial.deploymentId ?? env.WORKFLOW_DEPLOYMENT_ID ?? 'dpl_sheets_local',
    baseUrl: (partial.baseUrl ?? envBaseUrl()).replace(/\/$/, ''),
    startDispatcher:
      partial.startDispatcher ??
      (env.WORKFLOW_SHEETS_DISABLE_DISPATCHER ? false : true),
    dispatcherPollMs:
      partial.dispatcherPollMs ??
      numEnv(env.WORKFLOW_SHEETS_DISPATCHER_POLL_MS, 2000),
    maxAttempts:
      partial.maxAttempts ?? numEnv(env.WORKFLOW_SHEETS_MAX_ATTEMPTS, 10),
    retryBaseMs:
      partial.retryBaseMs ?? numEnv(env.WORKFLOW_SHEETS_RETRY_BASE_MS, 5000),
    recoverActiveRuns: partial.recoverActiveRuns ?? true,
    autoBootstrap: partial.autoBootstrap ?? true,
  };
}
