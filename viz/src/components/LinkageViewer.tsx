import { Table } from 'apache-arrow'
import RecordTable from './RecordTable'

export interface LinkageTables {
  left: Table
  right: Table
  links: Table
}


export function LinkageViewer({ left, right, links }: LinkageTables) {
  return (
    <div className="flex flex-col gap-8">
      <div>
        <h2 className="font-bold mb-2">Left Table</h2>
        <RecordTable table={left} />
      </div>
      <div>
        <h2 className="font-bold mb-2">Right Table</h2>
        <RecordTable table={right} />
      </div>
      <div>
        <h2 className="font-bold mb-2">Links Table</h2>
        <RecordTable table={links} />
      </div>
    </div>
  )
}


