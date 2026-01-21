import React, { useState } from 'react'
import axios from 'axios'

const BACKEND = process.env.BACKEND_URL || 'http://localhost:8000'

export default function App() {
  const [step, setStep] = useState(1)
  const [source, setSource] = useState({ vendor: 'postgres', host: 'localhost', port: 5432, database: '', user: '', password: '' })
  const [capture, setCapture] = useState({ table: '', watermark_column: '', polling_interval_ms: 5000 })
  const [destination, setDestination] = useState({ provider: 's3', bucket: 'delta-cdc', base_path: '' })
  const [name, setName] = useState('')
  const [message, setMessage] = useState('')

  async function submit() {
    const payload = { name, source, capture, destination, enabled: true }
    try {
      const r = await axios.post(`${BACKEND}/jobs`, payload)
      setMessage('Job created: ' + r.data.id)
    } catch (e) {
      setMessage('Error: ' + (e.response?.data?.detail || e.message))
    }
  }

  return (
    <div style={{ padding: 24, fontFamily: 'Arial, sans-serif' }}>
      <h1>delta-stream-capture (polling)</h1>
      {step === 1 && (
        <div>
          <h2>1) Source</h2>
          <label>Name: <input value={name} onChange={e => setName(e.target.value)} /></label>
          <div>
            <label>Vendor: <select value={source.vendor} onChange={e => setSource({ ...source, vendor: e.target.value })}>
              <option value="postgres">Postgres</option>
              <option value="mysql">MySQL</option>
            </select></label>
          </div>
          <div>
            <label>Host: <input value={source.host} onChange={e => setSource({ ...source, host: e.target.value })} /></label>
            <label>Port: <input value={source.port} onChange={e => setSource({ ...source, port: Number(e.target.value) })} /></label>
          </div>
          <div>
            <label>Database: <input value={source.database} onChange={e => setSource({ ...source, database: e.target.value })} /></label>
            <label>User: <input value={source.user} onChange={e => setSource({ ...source, user: e.target.value })} /></label>
            <label>Password: <input type="password" value={source.password} onChange={e => setSource({ ...source, password: e.target.value })} /></label>
          </div>
          <button onClick={() => setStep(2)}>Next</button>
        </div>
      )}

      {step === 2 && (
        <div>
          <h2>2) Capture</h2>
          <div>
            <label>Table (schema.table): <input value={capture.table} onChange={e => setCapture({ ...capture, table: e.target.value })} /></label>
          </div>
          <div>
            <label>Watermark column: <input value={capture.watermark_column} onChange={e => setCapture({ ...capture, watermark_column: e.target.value })} /></label>
          </div>
          <div>
            <label>Polling interval ms: <input value={capture.polling_interval_ms} onChange={e => setCapture({ ...capture, polling_interval_ms: Number(e.target.value) })} /></label>
          </div>
          <button onClick={() => setStep(1)}>Back</button>
          <button onClick={() => setStep(3)}>Next</button>
        </div>
      )}

      {step === 3 && (
        <div>
          <h2>3) Destination</h2>
          <div>
            <label>Bucket: <input value={destination.bucket} onChange={e => setDestination({ ...destination, bucket: e.target.value })} /></label>
            <label>Base path: <input value={destination.base_path} onChange={e => setDestination({ ...destination, base_path: e.target.value })} /></label>
          </div>
          <button onClick={() => setStep(2)}>Back</button>
          <button onClick={submit}>Create job</button>
        </div>
      )}

      {message && <div style={{ marginTop: 16 }}><strong>{message}</strong></div>}

    </div>
  )
}
