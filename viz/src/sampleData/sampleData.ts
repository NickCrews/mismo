import { tableFromIPC } from 'apache-arrow'

export async function getSampleLinkageTables() {
  const leftResp = await fetch('src/sampleData/left.arrow', { cache: 'force-cache' })
  const rightResp = await fetch('src/sampleData/right.arrow', { cache: 'force-cache' })
  const linksResp = await fetch('src/sampleData/links.arrow', { cache: 'force-cache' })
  const left = tableFromIPC(new Uint8Array(await leftResp.arrayBuffer()))
  const right = tableFromIPC(new Uint8Array(await rightResp.arrayBuffer()))
  const links = tableFromIPC(new Uint8Array(await linksResp.arrayBuffer()))
  return { left, right, links }
}
