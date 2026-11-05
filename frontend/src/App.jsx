import { useCallback, useEffect, useState } from 'react'
import {
  getBuckets,
  createBucket,
  getClusterStatus,
  getObjects,
  uploadObject,
  downloadObject,
  deleteObject,
  searchObjects,
} from './services/api'
import './App.css'

function App() {
  const [buckets, setBuckets] = useState([])
  const [selectedBucket, setSelectedBucket] = useState('')
  const [objects, setObjects] = useState([])
  const [cluster, setCluster] = useState(null)
  const [newBucket, setNewBucket] = useState('')
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState(false)
  const [searchPrefix, setSearchPrefix] = useState('')
  const [searchMinSize, setSearchMinSize] = useState('')
  const [searchMaxSize, setSearchMaxSize] = useState('')
  const [searchTagKey, setSearchTagKey] = useState('')
  const [searchTagValue, setSearchTagValue] = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [searching, setSearching] = useState(false)
  const [hasSearched, setHasSearched] = useState(false)

  const refreshDashboard = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const [bucketData, clusterData] = await Promise.all([
        getBuckets(),
        getClusterStatus(),
      ])
      setBuckets(bucketData.buckets || [])
      setCluster(clusterData)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }, [])

  async function selectBucket(name) {
    setSelectedBucket(name)
    setObjects([])
    setError('')
    try {
      const data = await getObjects(name)
      setObjects(data.objects || [])
    } catch (err) {
      setError(err.message)
    }
  }

  async function refreshObjects(bucket) {
    const data = await getObjects(bucket)
    setObjects(data.objects || [])
  }

  async function handleRefresh() {
    await refreshDashboard()
    if (selectedBucket) {
      try {
        await refreshObjects(selectedBucket)
      } catch (err) {
        setError(err.message)
      }
    }
  }

  async function handleCreateBucket(event) {
    event.preventDefault()
    const name = newBucket.trim()
    if (!name) return
    setBusy(true)
    setError('')
    setNotice('')
    try {
      await createBucket(name)
      setNewBucket('')
      await refreshDashboard()
      await selectBucket(name)
      setNotice(`Bucket "${name}" is ready.`)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  async function handleUpload(event) {
    const file = event.target.files?.[0]
    if (!file || !selectedBucket) return
    const bucket = selectedBucket
    setBusy(true)
    setError('')
    setNotice('')
    try {
      await uploadObject(bucket, file)
      await refreshObjects(bucket)
      await refreshDashboard()
      setNotice(`Uploaded "${file.name}" successfully.`)
    } catch (err) {
      setError(err.message)
    } finally {
      event.target.value = ''
      setBusy(false)
    }
  }

  async function handleDownload(key) {
    setError('')
    try {
      await downloadObject(selectedBucket, key)
    } catch (err) {
      setError(err.message)
    }
  }

  async function handleDelete(key) {
    if (!window.confirm(`Delete "${key}" from "${selectedBucket}"?`)) return
    setBusy(true)
    setError('')
    setNotice('')
    try {
      await deleteObject(selectedBucket, key)
      await refreshObjects(selectedBucket)
      await refreshDashboard()
      setNotice(`Deleted "${key}" successfully.`)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  async function handleSearch(event) {
    event.preventDefault()
    setSearching(true)
    setError('')
    setHasSearched(false)
    try {
      if ((searchTagKey.trim() === '') !== (searchTagValue.trim() === '')) {
        throw new Error('Enter both tag key and tag value, or leave both empty.')
      }
      const data = await searchObjects({
        prefix: searchPrefix.trim(),
        min_size: searchMinSize,
        max_size: searchMaxSize,
        tag_key: searchTagKey.trim(),
        tag_value: searchTagValue.trim(),
      })
      setSearchResults(data.results || [])
      setHasSearched(true)
    } catch (err) {
      setSearchResults([])
      setError(err.message)
    } finally {
      setSearching(false)
    }
  }

  useEffect(() => {
    refreshDashboard()
  }, [refreshDashboard])

  return (
    <div className="dashboard">
      <header className="topbar">
        <div>
          <h1>Distributed Storage Dashboard</h1>
          <p>Object Storage Management System</p>
        </div>
        <button onClick={handleRefresh} disabled={loading || busy}>
          {loading ? 'Refreshing...' : 'Refresh'}
        </button>
      </header>

      {error && <div className="error" role="alert">{error}</div>}
      {notice && <div className="notice" role="status">{notice}</div>}

      <section className="stats">
        <div className="stat-card"><span>Storage Nodes</span><strong>{cluster ? `${cluster.healthy_nodes}/${cluster.total_nodes}` : '—'}</strong></div>
        <div className="stat-card"><span>Total Buckets</span><strong>{buckets.length}</strong></div>
        <div className="stat-card"><span>Degraded Objects</span><strong>{cluster?.degraded_objects ?? '—'}</strong></div>
        <div className="stat-card"><span>Unavailable Objects</span><strong>{cluster?.unavailable_objects ?? '—'}</strong></div>
      </section>

      <div className="content-grid">
        <section className="panel">
          <h2>Buckets</h2>
          <form onSubmit={handleCreateBucket} className="bucket-form">
            <input value={newBucket} onChange={(e) => setNewBucket(e.target.value)} placeholder="New bucket name" required />
            <button type="submit" disabled={busy}>Create</button>
          </form>
          <div className="bucket-list">
            {buckets.length === 0 && <p>No buckets found.</p>}
            {buckets.map((bucket) => (
              <button key={bucket} className={selectedBucket === bucket ? 'selected' : ''} onClick={() => selectBucket(bucket)} disabled={busy}>
                📁 {bucket}
              </button>
            ))}
          </div>
        </section>

        <section className="panel objects-panel">
          <h2>Objects {selectedBucket && `— ${selectedBucket}`}</h2>
          {selectedBucket && (
            <div className="upload-area">
              <label htmlFor="file-upload">Upload a file</label>
              <input id="file-upload" type="file" onChange={handleUpload} disabled={busy} />
              {busy && <small>Processing request...</small>}
            </div>
          )}
          {!selectedBucket ? (
            <p>Select a bucket to view its objects.</p>
          ) : objects.length === 0 ? (
            <p>No objects in this bucket.</p>
          ) : (
            <div className="table-scroll">
              <table>
                <thead><tr><th>Object Name</th><th>Size</th><th>Version</th><th>Actions</th></tr></thead>
                <tbody>
                  {objects.map((object) => (
                    <tr key={object.key}>
                      <td>{object.key}</td>
                      <td>{object.size.toLocaleString()} bytes</td>
                      <td>{object.version}</td>
                      <td className="actions">
                        <button onClick={() => handleDownload(object.key)} disabled={busy}>Download</button>
                        <button className="delete-button" onClick={() => handleDelete(object.key)} disabled={busy}>Delete</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>

      <section className="panel search-panel">
        <h2>Object Search</h2>
        <p className="search-hint">Search stored objects across all buckets using Elasticsearch.</p>
        <form className="search-form" onSubmit={handleSearch}>
          <input aria-label="Filename prefix" placeholder="Filename prefix (e.g. test)" value={searchPrefix} onChange={(e) => setSearchPrefix(e.target.value)} />
          <input aria-label="Minimum size in bytes" type="number" min="0" placeholder="Min bytes" value={searchMinSize} onChange={(e) => setSearchMinSize(e.target.value)} />
          <input aria-label="Maximum size in bytes" type="number" min="0" placeholder="Max bytes" value={searchMaxSize} onChange={(e) => setSearchMaxSize(e.target.value)} />
          <input aria-label="Tag key" placeholder="Tag key (optional)" value={searchTagKey} onChange={(e) => setSearchTagKey(e.target.value)} />
          <input aria-label="Tag value" placeholder="Tag value (optional)" value={searchTagValue} onChange={(e) => setSearchTagValue(e.target.value)} />
          <button type="submit" disabled={searching}>{searching ? 'Searching...' : 'Search'}</button>
        </form>
        {hasSearched && (
          <div className="search-results">
            <h3>Search Results ({searchResults.length})</h3>
            {searchResults.length === 0 ? (
              <p>No matching objects found.</p>
            ) : (
              <div className="table-scroll">
                <table>
                  <thead><tr><th>Bucket</th><th>Object Name</th><th>Size</th><th>Created At</th></tr></thead>
                  <tbody>
                    {searchResults.map((object) => (
                      <tr key={`${object.bucket}/${object.key}`}>
                        <td>{object.bucket}</td>
                        <td>{object.key}</td>
                        <td>{Number(object.size).toLocaleString()} bytes</td>
                        <td>{new Date(object.created_at).toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </section>

      <section className="panel">
        <h2>Cluster Health</h2>
        <div className="nodes">
          {cluster?.nodes?.map((node) => (
            <div key={node.id} className="node-card">
              <strong>{node.id}</strong>
              <span className={node.healthy ? 'healthy' : 'unhealthy'}>{node.healthy ? '● Healthy' : '● Offline'}</span>
              <small>Chunks: {node.chunk_count ?? '—'}</small>
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}

export default App
