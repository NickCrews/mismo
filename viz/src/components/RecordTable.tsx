import { useMemo, useState } from 'react'
import { Table as ArrowTable } from 'apache-arrow'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  type ColumnDef,
} from '@tanstack/react-table'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

function arrowTableToColumnDefs(table: ArrowTable): ColumnDef<any, any>[] {
  return table.schema.fields.map((field) => ({
    accessorKey: field.name,
    header: field.name,
  }))
}

export default function RecordTable({ table }: { table: ArrowTable }) {
  const data = useMemo(() => {console.log("regetting records"); return table.toArray().slice(0, 100)}, [table])
  const [columns] = useState(() => arrowTableToColumnDefs(table))

  const tanTable = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    columnResizeMode: 'onChange',
    // debugTable: true,
    // debugHeaders: true,
    // debugColumns: true,
  })

  return (
    <>
    <Table className="text-xs" {...{style: {
        width: tanTable.getCenterTotalSize(),
    }}}>
      <TableHeader>
        {tanTable.getHeaderGroups().map((headerGroup) => (
          <TableRow key={headerGroup.id}>
            {headerGroup.headers.map((header) => (
              <TableHead key={header.id}>
                <div className="flex items-center group relative" style={{ width: header.getSize()}}>
                  {flexRender(header.column.columnDef.header, header.getContext())}
                  {header.column.getCanResize() && (
                    <div
                      onMouseDown={header.getResizeHandler()}
                      onTouchStart={header.getResizeHandler()}
                      className={`absolute right-0 top-0 h-full w-1.5 bg-gray cursor-col-resize select-none group-hover:bg-gray-300 ${header.column.getIsResizing() ? 'bg-gray-400' : 'bg-gray-200'}`}
                      data-resize-handle
                    />
                  )}
                </div>
              </TableHead>
            ))}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {tanTable.getRowModel().rows.map((row) => (
          <TableRow key={row.id}>
            {row.getVisibleCells().map((cell) => (
              <TableCell
                key={cell.id}
                style={{
                    width: cell.column.getSize(),
                    maxWidth: cell.column.getSize(),
                }}
                className="overflow-hidden text-ellipsis whitespace-nowrap"
              >
                {flexRender(cell.column.columnDef.cell, cell.getContext())}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
    </>
  )
}