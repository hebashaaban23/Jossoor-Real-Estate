// apps/crm/frontend/src/utils/commentType.js
export const TYPE_OPTIONS = ['Call', 'WhatsApp', 'Meeting', 'Property Showing']

export function buildContent(text, type) {
  const clean = String(text || '').trim()
  const t = String(type || '').trim()
  const withoutPrefix = clean.replace(/^\s*\[(Call|WhatsApp|Meeting|Property Showing)\]\s*/i, '')
  return (t ? `[${t}] ` : '') + withoutPrefix
}

export function parseType(content) {
  const s = String(content || '')
  const m = s.match(/^\s*\[(Call|WhatsApp|Meeting|Property Showing)\]\s*/i)
  if (m) {
    const type = m[1][0].toUpperCase() + m[1].slice(1).toLowerCase()
    return { type, text: s.replace(m[0], '').trim() }
  }
  return { type: '', text: s.trim() }
}

