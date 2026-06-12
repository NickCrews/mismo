import { StrictMode, useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'

import { LinkageViewer, type LinkageTables } from './components/LinkageViewer'
import { getSampleLinkageTables } from './sampleData/sampleData'

function App() {
  const [tables, setTables] = useState<LinkageTables | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getSampleLinkageTables()
      .then(setTables)
      .catch((e) => setError(e.message || 'Failed to load tables'))
  }, [])

  if (error) return <div>Error: {error}</div>
  if (!tables) return <div>Loading...</div>
  return (
      <LinkageViewer left={tables.left} right={tables.right} links={tables.links} />
  )
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
