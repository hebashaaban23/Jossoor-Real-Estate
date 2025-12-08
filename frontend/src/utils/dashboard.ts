import { dayjs } from "frappe-ui"

export function getLastXDays(range: number = 30): string | null {
  const today = new Date()
  const lastXDate = new Date(today)
  lastXDate.setDate(today.getDate() - range)

  return `${dayjs(lastXDate).format('YYYY-MM-DD')},${dayjs(today).format(
    'YYYY-MM-DD',
  )}`
}

export function getToday(): string {
  const today = new Date()
  return `${dayjs(today).format('YYYY-MM-DD')},${dayjs(today).format('YYYY-MM-DD')}`
}

export function getYesterday(): string {
  const today = new Date()
  const yesterday = new Date(today)
  yesterday.setDate(today.getDate() - 1)
  return `${dayjs(yesterday).format('YYYY-MM-DD')},${dayjs(yesterday).format('YYYY-MM-DD')}`
}

export function formatter(range: string) {
  let [from, to] = range.split(',')
  return `${formatRange(from)} to ${formatRange(to)}`
}

export function formatRange(date: string) {
  const dateObj = new Date(date)
  return dateObj.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year:
      dateObj.getFullYear() === new Date().getFullYear()
        ? undefined
        : 'numeric',
  })
}
