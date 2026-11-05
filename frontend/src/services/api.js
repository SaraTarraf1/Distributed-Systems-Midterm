const BASE_URL = '/api'
const API_KEY = import.meta.env.VITE_API_KEY || 'midterm-secret'

function objectPath(bucket, key) {
  return `/objects/${encodeURIComponent(bucket)}/${key.split('/').map(encodeURIComponent).join('/')}`
}

export async function apiRequest(path, options = {}) {
  const response = await fetch(`${BASE_URL}${path}`, {
    ...options,
    headers: {
      'X-API-Key': API_KEY,
      ...(options.headers || {}),
    },
  })
  if (!response.ok) {
    const message = await response.text()
    throw new Error(`API error ${response.status}: ${message}`)
  }
  return response.json()
}

export const getBuckets = () => apiRequest('/buckets')
export const createBucket = (name) =>
  apiRequest(`/buckets/${encodeURIComponent(name)}`, { method: 'POST' })
export const getClusterStatus = () => apiRequest('/cluster/status?scan_objects=true')
export const getObjects = (bucket) =>
  apiRequest(`/buckets/${encodeURIComponent(bucket)}/objects`)

export async function uploadObject(bucket, file) {
  const formData = new FormData()
  formData.append('file', file)
  return apiRequest(objectPath(bucket, file.name), {
    method: 'PUT',
    body: formData,
  })
}

export const deleteObject = (bucket, key) =>
  apiRequest(objectPath(bucket, key), { method: 'DELETE' })

export async function downloadObject(bucket, key) {
  const response = await fetch(`${BASE_URL}${objectPath(bucket, key)}`, {
    headers: { 'X-API-Key': API_KEY },
  })
  if (!response.ok) {
    const message = await response.text()
    throw new Error(`Download failed ${response.status}: ${message}`)
  }
  const blob = await response.blob()
  const url = URL.createObjectURL(blob)
  try {
    const link = document.createElement('a')
    link.href = url
    link.download = key.split('/').pop() || 'download'
    document.body.appendChild(link)
    link.click()
    link.remove()
  } finally {
    setTimeout(() => URL.revokeObjectURL(url), 1000)
  }
}

// PostgreSQL-backed search via the existing FastAPI /search endpoint.
// Elasticsearch-backed object search.
export function searchObjects(filters = {}) {
  const params = new URLSearchParams()

  for (const [key, value] of Object.entries(filters)) {
    if (value !== '' && value !== null && value !== undefined) {
      params.set(key, String(value))
    }
  }

  const query = params.toString()

  return apiRequest(
    `/search/elasticsearch${query ? `?${query}` : ''}`
  )
}
