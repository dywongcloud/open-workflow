export default function Home() {
  return (
    <main style={{ fontFamily: 'system-ui', padding: 32, lineHeight: 1.6 }}>
      <h1>open-workflow · Next.js</h1>
      <p>Durable workflows backed by Redis + 307 redirects — vendor-agnostic.</p>
      <h2>Try it</h2>
      <pre>
        {`# start a run
curl -XPOST localhost:3000/api/run -d '{"name":"Ada"}'

# poll status (use the returned runId)
curl 'localhost:3000/api/status?runId=<runId>'`}
      </pre>
    </main>
  );
}
