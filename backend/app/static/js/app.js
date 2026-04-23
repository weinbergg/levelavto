;(function () {
  const qs = (s, el = document) => el.querySelector(s)
  const qsa = (s, el = document) => Array.from(el.querySelectorAll(s))
  if (window.DEBUG_PERF && !window.__fetchWrapped) {
    window.__fetchWrapped = true
    const origFetch = window.fetch.bind(window)
    const seen = new Map()
    window.fetch = async (...args) => {
      const url = (args[0] && args[0].toString) ? args[0].toString() : String(args[0] || '')
      const start = performance.now()
      const res = await origFetch(...args)
      const ms = performance.now() - start
      const cnt = (seen.get(url) || 0) + 1
      seen.set(url, cnt)
      console.log(`[fetch] ${res.status} ${ms.toFixed(1)}ms count=${cnt} url=${url}`)
      return res
    }
  }

  // --- FX rates (RUB) ---
  let fxCache = { loaded: false, rates: {} }

  async function getFx() {
    if (fxCache.loaded) return fxCache
    try {
      const res = await fetch('https://www.cbr-xml-daily.ru/daily_json.js', { cache: 'no-cache' })
      const data = await res.json()
      fxCache = {
        loaded: true,
        rates: {
          EUR: data?.Valute?.EUR?.Value,
          USD: data?.Valute?.USD?.Value,
        },
      }
    } catch (e) {
      console.warn('fx rates', e)
      fxCache = { loaded: true, rates: {} }
    }
    return fxCache
  }

  function priceToRub(price, currency, fx) {
    if (price == null) return null
    const cur = (currency || 'EUR').toString().trim().toUpperCase()
    if (!cur || cur === 'RUB' || cur === '₽') return Number(price)
    const rate = fx?.rates?.[cur]
    if (!rate) return null
    return Number(price) * Number(rate)
  }

  function formatRub(val) {
    const n = Number(val)
    if (!Number.isFinite(n)) return '—'
    return `${Math.round(n).toLocaleString('ru-RU')} ₽`
  }

  function animateCount(el, nextValue, duration = 400) {
    if (!el) return
    const from = Number(el.dataset.count || el.textContent || 0)
    const to = Number(nextValue || 0)
    if (!Number.isFinite(from) || !Number.isFinite(to)) {
      el.textContent = String(nextValue || 0)
      el.dataset.count = String(nextValue || 0)
      return
    }
    const start = performance.now()
    const tick = (now) => {
      const t = Math.min(1, (now - start) / duration)
      const value = Math.round(from + (to - from) * t)
      el.textContent = value.toLocaleString('ru-RU')
      if (t < 1) {
        requestAnimationFrame(tick)
      } else {
        el.dataset.count = String(to)
      }
    }
    requestAnimationFrame(tick)
  }

  function escapeHtml(value) {
    return String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/\"/g, '&quot;')
      .replace(/'/g, '&#39;')
  }

  function buildCatalogUrl(params) {
    const qs = params.toString()
    return qs ? `/catalog?${qs}` : '/catalog'
  }

  const THUMB_REV = '4'
  const DETAIL_PRIMARY_WIDTH = 640

  function tryParseUrl(url) {
    try {
      return new URL(url, window.location.origin)
    } catch (_) {
      return null
    }
  }

  function isThumbProxyPath(pathname) {
    return pathname === '/thumb' || pathname.endsWith('/thumb')
  }

  function shouldProxyThumbSource(url) {
    const raw = String(url || '')
    return raw.includes('img.classistatic.de') || raw.includes('autoimg.cn')
  }

  function normalizeThumbSourceUrl(uRaw) {
    const u = String(uRaw || '').trim()
    if (!u) return '/static/img/no-photo.svg'
    if (u.startsWith('/media/') || u.startsWith('/static/')) return u
    if (u.startsWith('//')) return `https:${u}`
    if (u.startsWith('http://')) return u.replace('http://', 'https://')
    if (u.startsWith('https://')) return u
    if (u.startsWith('/api/v1/mo-prod/images/')) return `https://img.classistatic.de${u}`
    if (u.startsWith('api/v1/mo-prod/images/')) return `https://img.classistatic.de/${u}`
    if (u.startsWith('img.classistatic.de/')) return `https://${u}`
    return u
  }

  function normalizeThumbProxy(url) {
    const parsed = tryParseUrl(url)
    if (!parsed || !isThumbProxyPath(parsed.pathname)) return null
    const u = normalizeThumbSourceUrl(parsed.searchParams.get('u'))
    if (!u || u === '/static/img/no-photo.svg') return '/static/img/no-photo.svg'
    // Local/static files should never go through /thumb proxy.
    if (u.startsWith('/media/') || u.startsWith('/static/')) return u
    if (!shouldProxyThumbSource(u)) return u
    return `/thumb?u=${encodeURIComponent(u)}&w=${parsed.searchParams.get('w') || '360'}&fmt=${parsed.searchParams.get('fmt') || 'webp'}&rev=${THUMB_REV}`
  }

  function thumbProxyUrl(url, width = 360) {
    if (!url) return '/static/img/no-photo.svg'
    const normalizedProxy = normalizeThumbProxy(url)
    if (normalizedProxy) return normalizedProxy
    if (url.startsWith('/thumb?')) {
      return url.includes('rev=') ? url : `${url}&rev=${THUMB_REV}`
    }
    if (!shouldProxyThumbSource(url)) return url
    return `/thumb?u=${encodeURIComponent(url)}&w=${width}&fmt=webp&rev=${THUMB_REV}`
  }

  function normalizeThumbUrl(src, opts = {}) {
    const val = String(src || '').trim()
    if (!val) return '/static/img/no-photo.svg'
    let url = val
    while (url.startsWith('.')) url = url.slice(1)
    const normalizedProxy = normalizeThumbProxy(url)
    if (normalizedProxy) {
      if (opts.thumb) return normalizedProxy
      const parsed = tryParseUrl(normalizedProxy)
      if (!parsed) return normalizedProxy
      const u = parsed.searchParams.get('u')
      return u ? normalizeThumbSourceUrl(u) : normalizedProxy
    }
    if (url.startsWith('//')) url = `https:${url}`
    if (url.startsWith('http://')) url = url.replace('http://', 'https://')
    if (url.startsWith('api/v1/mo-prod/images/')) url = `https://img.classistatic.de/${url}`
    if (url.startsWith('img.classistatic.de/')) url = `https://${url}`
    if (!url.startsWith('https://') && !url.startsWith('/')) {
      const uuidRe = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
      if (uuidRe.test(url)) {
        const prefix = url.slice(0, 2)
        url = `https://img.classistatic.de/api/v1/mo-prod/images/${prefix}/${url}?rule=mo-1024.jpg`
      } else {
        return '/static/img/no-photo.svg'
      }
    }
    // Keep original classistatic rule to avoid 404 on some sizes.
    if (opts.thumb) {
      return thumbProxyUrl(url, Number(opts.width) || 360)
    }
    return url
  }

  function applyThumbFallback(img, opts = {}) {
    if (!img) return
    const useThumbProxy = opts.thumbProxy !== false
    const rawSrc = img.getAttribute('src') || ''
    const rawData = img.dataset.thumb || ''
    let normalized = normalizeThumbUrl(rawSrc, { thumb: useThumbProxy })
    if (normalized === '/static/img/no-photo.svg' && rawData) {
      const dataNormalized = normalizeThumbUrl(rawData, { thumb: useThumbProxy })
      if (dataNormalized !== '/static/img/no-photo.svg') {
        normalized = dataNormalized
      }
    }
    if (img.getAttribute('src') !== normalized) {
      img.setAttribute('src', normalized)
    }
    if (!img.getAttribute('referrerpolicy')) {
      img.setAttribute('referrerpolicy', 'no-referrer')
    }
    if (!img.getAttribute('loading')) {
      img.setAttribute('loading', 'lazy')
    }
    if (!img.getAttribute('decoding')) {
      img.setAttribute('decoding', 'async')
    }
    if (!img.dataset.fallbackBound) {
      img.dataset.fallbackBound = '1'
      img.onerror = () => {
        const currentSrc = img.getAttribute('src') || ''
        const currentThumb = normalizeThumbUrl(img.dataset.thumb || '', { thumb: true })
        if (!img.dataset.thumbFallbackTried && currentThumb && currentThumb !== '/static/img/no-photo.svg' && currentThumb !== currentSrc) {
          img.dataset.thumbFallbackTried = '1'
          img.src = currentThumb
          return
        }
        // First failure on proxy thumb can be transient (e.g. lock busy -> 503).
        // Retry once with cache-busting before falling back to static placeholder.
        if (!img.dataset.thumbRetried && currentSrc.startsWith('/thumb?')) {
          img.dataset.thumbRetried = '1'
          const retrySrc = `${currentSrc}${currentSrc.includes('?') ? '&' : '?'}rt=${Date.now()}`
          setTimeout(() => {
            img.src = retrySrc
          }, 120)
          return
        }
        if (!img.dataset.origRetried) {
          const origRaw = img.dataset.orig || ''
          const origSrc = normalizeThumbUrl(origRaw, { thumb: false })
          if (origSrc && origSrc !== '/static/img/no-photo.svg' && origSrc !== currentSrc) {
            img.dataset.origRetried = '1'
            img.src = origSrc
            return
          }
        }
        if (img.dataset.fallbackApplied) return
        img.dataset.fallbackApplied = '1'
        img.src = '/static/img/no-photo.svg'
      }
    }
  }

  function setSelectOptions(select, items, { emptyLabel = 'Все', valueKey = 'value', labelKey = 'label' } = {}) {
    if (!select) return
    const current = select.value
    const seenValues = new Set()
    const normalizedItems = (items || []).filter((item) => {
      const isObj = item && typeof item === 'object'
      const value = isObj ? (item[valueKey] ?? item.value) : item
      if (value == null || value === '') return false
      const key = String(value)
      if (seenValues.has(key)) return false
      seenValues.add(key)
      return true
    })
    select.innerHTML = ''
    const emptyOpt = document.createElement('option')
    emptyOpt.value = ''
    emptyOpt.textContent = emptyLabel
    select.appendChild(emptyOpt)
    normalizedItems.forEach((item) => {
      const isObj = item && typeof item === 'object'
      const value = isObj ? (item[valueKey] ?? item.value) : item
      if (value == null || value === '') return
      const label = isObj ? (item[labelKey] ?? item.label ?? value) : value
      const opt = document.createElement('option')
      opt.value = value
      opt.textContent = label
      select.appendChild(opt)
    })
    if (current) {
      const match = Array.from(select.options).find((o) => o.value === current)
      if (match) select.value = current
    }
    select.disabled = normalizedItems.length === 0
  }

  function ensureOption(select, value, label = null) {
    if (!select || !value) return
    const exists = Array.from(select.options || []).some((o) => o.value === value)
    if (exists) return
    const opt = document.createElement('option')
    opt.value = value
    opt.textContent = label || value
    select.appendChild(opt)
  }

  function renderColorChips(container, colors) {
    if (!container) return
    container.innerHTML = ''
    const seen = new Set()
    ;(colors || []).forEach((c) => {
      if (!c || !c.value) return
      const key = String(c.value || '').trim()
      if (!key || seen.has(key)) return
      seen.add(key)
      const btn = document.createElement('button')
      btn.type = 'button'
      btn.className = 'color-chip'
      btn.dataset.color = c.value
      btn.dataset.label = c.label
      btn.title = c.label
      btn.setAttribute('aria-label', c.label)
      if (c.hex) btn.style.setProperty('--chip-color', c.hex)
      const text = document.createElement('span')
      text.className = 'color-chip__text'
      text.textContent = c.label || c.value
      btn.appendChild(text)
      container.appendChild(btn)
    })
  }

  function diffParams(a, b) {
    const out = {}
    const keys = new Set([...a.keys(), ...b.keys()])
    keys.forEach((key) => {
      const left = a.get(key) || ''
      const right = b.get(key) || ''
      if (left !== right) {
        out[key] = { left, right }
      }
    })
    return out
  }

  const DEBUG_FILTERS = localStorage.getItem('debugFilters') === '1'
  if (DEBUG_FILTERS) {
    const testUrl = buildCatalogUrl(new URLSearchParams({ brand: 'Cadillac', model: 'CT6' }))
    console.assert(testUrl.includes('brand=Cadillac') && testUrl.includes('model=CT6'), 'buildCatalogUrl failed', testUrl)
  }

  function normalizeBrand(value) {
    const raw = String(value || '').trim()
    if (!raw) return ''
    const key = raw.toLowerCase()
    if (key === 'alfa' || key === 'alfa romeo') return 'Alfa Romeo'
    return raw
  }

  function normalizeBrandOptions(select) {
    if (!select) return
    const options = Array.from(select.options || [])
    const alfa = options.find((opt) => opt.value?.toLowerCase?.() === 'alfa')
    const alfaRomeo = options.find((opt) => opt.value?.toLowerCase?.() === 'alfa romeo')
    if (alfa && alfaRomeo) {
      alfa.remove()
      return
    }
    if (alfa && !alfaRomeo) {
      alfa.value = 'Alfa Romeo'
      alfa.textContent = 'Alfa Romeo'
    }
  }

  function formatPrice(price, currency, fx) {
    const rub = priceToRub(price, currency, fx)
    if (rub != null) return formatRub(rub)
    if (price == null) return ''
    return `${Number(price).toLocaleString('ru-RU')} ${currency || ''}`
  }

  function getFilters(page) {
    const form = qs('#filters')
    const data = new FormData(form)
    const params = new URLSearchParams()
    for (const [k, v] of data.entries()) {
      if (v) params.append(k, v)
    }
    params.set('page', String(page || 1))
    params.set('page_size', '12')
    const pageField = form.querySelector('input[name=\"page\"]')
    if (pageField) pageField.value = String(page || 1)
    return params
  }

  function applyQueryToFilters() {
    const form = qs('#filters')
    if (!form) return
    const params = new URLSearchParams(window.location.search)
    params.forEach((value, key) => {
      if (key === 'line') return
      const field = form.elements[key]
      if (!field) return
      if (field.tagName === 'SELECT' || field.tagName === 'INPUT') {
        const nextValue = key === 'brand' ? normalizeBrand(value) : value
        field.value = nextValue
      }
    })
    syncColorChips(form)
    syncChoiceChips(form)
    syncMultiSelectMenus(form)
    syncRegMonthState(form)
  }

  function parseSelectedFilters() {
    const params = new URLSearchParams(window.location.search)
    const selected = new URLSearchParams()
    if (!params.get('country') && params.get('eu_country')) {
      params.set('country', params.get('eu_country'))
    }
    if (params.get('eu_country')) {
      params.delete('eu_country')
    }
    params.forEach((value, key) => {
      if (!value) return
      let next = value
      if (key === 'region' || key === 'country' || key === 'kr_type') {
        next = String(value).trim().toUpperCase()
      }
      if (key === 'brand') {
        next = normalizeBrand(value)
      }
      selected.append(key, next)
    })
    const lineItems = params
      .getAll('line')
      .map((line) => parseLineValue(line))
      .filter((item) => item.brand || item.model)
    if (!selected.get('brand')) {
      const lineBrands = Array.from(new Set(lineItems.map((item) => item.brand).filter(Boolean)))
      if (lineBrands.length === 1) {
        selected.set('brand', lineBrands[0])
      }
    }
    if (!selected.get('model')) {
      const lineModels = Array.from(new Set(lineItems.map((item) => item.model).filter(Boolean)))
      if (lineModels.length === 1) {
        selected.set('model', lineModels[0])
      }
    }
    return selected
  }

  function parseLineValue(line) {
    const parts = String(line || '').split('|')
    return {
      brand: normalizeBrand(parts[0] || ''),
      model: String(parts[1] || '').trim(),
      variant: String(parts[2] || '').trim(),
    }
  }

  function groupLineSelections(lines = []) {
    const grouped = new Map()
    ;(lines || []).forEach((entry) => {
      const item = typeof entry === 'string' ? parseLineValue(entry) : entry
      const brand = normalizeBrand(item?.brand || '')
      const model = String(item?.model || '').trim()
      const variant = String(item?.variant || '').trim()
      const key = `${brand}__${variant}`
      if (!grouped.has(key)) {
        grouped.set(key, { brand, variant, models: [] })
      }
      if (model) {
        const bucket = grouped.get(key)
        if (!bucket.models.includes(model)) {
          bucket.models.push(model)
        }
      }
    })
    return Array.from(grouped.values())
  }

  function clearCatalogModelLineInputs(form) {
    if (!form || form.id !== 'filters') return
    qsa('input[name="line"]', form).forEach((el) => el.remove())
  }

  function setCatalogModelLineInputs(form, lines = []) {
    if (!form || form.id !== 'filters') return
    clearCatalogModelLineInputs(form)
    ;(lines || []).forEach((line) => {
      const value = String(line || '').trim()
      if (!value) return
      const input = document.createElement('input')
      input.type = 'hidden'
      input.name = 'line'
      input.value = value
      input.dataset.catalogLine = '1'
      form.appendChild(input)
    })
  }

  function getCatalogSelectedModels(form, brand = '') {
    if (!form || form.id !== 'filters') return []
    const normalizedBrand = normalizeBrand(brand || form.elements['brand']?.value || '')
    return qsa('input[name="line"][data-catalog-line="1"]', form)
      .map((el) => parseLineValue(el.value))
      .filter((item) => item.model && (!normalizedBrand || !item.brand || item.brand === normalizedBrand))
      .map((item) => item.model)
  }

  function getEffectiveCatalogSelectedModels(form) {
    if (!form || form.id !== 'filters') return []
    const modelSelect = form.elements['model'] || qs('#model-select', form)
    const stored = getAccordionSelectedModels(modelSelect)
    if (stored.length) return stored
    const fromLines = getCatalogSelectedModels(form)
    if (fromLines.length) return fromLines
    const fallback = String(modelSelect?.value || '').trim()
    return fallback ? [fallback] : []
  }

  function syncCatalogLinesFromState(form) {
    if (!form || form.id !== 'filters') return []
    const brand = normalizeBrand(form.elements['brand']?.value || '')
    const selectedModels = getEffectiveCatalogSelectedModels(form)
    if (!brand || selectedModels.length <= 1) {
      clearCatalogModelLineInputs(form)
      return []
    }
    const lines = selectedModels.map((model) => `${brand}|${String(model || '').trim()}|`).filter(Boolean)
    setCatalogModelLineInputs(form, lines)
    return lines
  }

  function setAccordionSelectedModels(select, values = []) {
    if (!select) return
    const normalized = Array.from(new Set((values || []).map((value) => String(value || '').trim()).filter(Boolean)))
    select.__accordionSelectedModels = normalized
    select.dataset.selectedModels = JSON.stringify(normalized)
  }

  function getAccordionSelectedModels(select) {
    if (!select) return []
    if (Array.isArray(select.__accordionSelectedModels)) {
      return select.__accordionSelectedModels.map((value) => String(value || '').trim()).filter(Boolean)
    }
    try {
      const parsed = JSON.parse(select.dataset.selectedModels || '[]')
      if (Array.isArray(parsed)) {
        const normalized = parsed.map((value) => String(value || '').trim()).filter(Boolean)
        select.__accordionSelectedModels = normalized
        return normalized
      }
    } catch {
      return []
    }
    return []
  }

  function syncFormFromSelected(form, selected) {
    if (!form || !selected) return
    setCatalogModelLineInputs(form, selected.getAll('line'))
    selected.forEach((value, key) => {
      if (key === 'line') return
      const field = form.elements[key]
      if (!field) return
      const nextValue = key === 'brand' ? normalizeBrand(value) : value
      if (field.tagName === 'SELECT') {
        const matched = setSelectValueInsensitive(field, nextValue)
        if (!matched) field.value = nextValue
      } else {
        field.value = nextValue
      }
    })
    syncColorChips(form)
    syncChoiceChips(form)
    syncMultiSelectMenus(form)
    syncRegMonthState(form)
  }

  function updateCatalogUrlFromParams(params) {
    if (!params) return
    params.delete('page')
    params.delete('page_size')
    const qs = params.toString()
    const next = qs ? `/catalog?${qs}` : '/catalog'
    window.history.replaceState(null, '', next)
  }

  function parseSelectedCsvValues(inputOrValue) {
    const raw = typeof inputOrValue === 'string'
      ? inputOrValue
      : (inputOrValue?.value || '')
    const values = []
    const seen = new Set()
    String(raw || '').split(',').forEach((item) => {
      const value = String(item || '').trim()
      if (!value) return
      const key = value.toLowerCase()
      if (seen.has(key)) return
      seen.add(key)
      values.push(value)
    })
    return values
  }

  function setSelectedCsvValues(input, values) {
    if (!input) return []
    const cleaned = parseSelectedCsvValues(Array.isArray(values) ? values.join(',') : String(values || ''))
    input.value = cleaned.join(',')
    return cleaned
  }

  function parseSelectedColorValues(inputOrValue) {
    return parseSelectedCsvValues(inputOrValue)
  }

  function setSelectedColorValues(input, values) {
    return setSelectedCsvValues(input, values)
  }

  function syncColorChips(scope) {
    if (!scope) return
    const input = qs('input[name="color"]', scope)
    const chips = qsa('.color-chip', scope)
    if (!input || !chips.length) return
    const selected = new Set(parseSelectedColorValues(input))
    chips.forEach((chip) => {
      chip.classList.toggle('active', selected.has(chip.dataset.color || ''))
    })
  }

  function bindColorChips(scope, onChange) {
    if (!scope) return
    const input = qs('input[name="color"]', scope)
    const chips = qsa('.color-chip', scope)
    if (!input || !chips.length) return
    chips.forEach((chip) => {
      if (chip.dataset.bound) return
      chip.dataset.bound = '1'
      chip.addEventListener('click', () => {
        const next = chip.dataset.color || ''
        const selected = parseSelectedColorValues(input)
        const hasNext = selected.includes(next)
        const updated = hasNext
          ? selected.filter((value) => value !== next)
          : [...selected, next]
        setSelectedColorValues(input, updated)
        syncColorChips(scope)
        bindOtherColorsToggle(scope)
        if (typeof onChange === 'function') onChange()
      })
    })
    syncColorChips(scope)
  }

  function syncOtherColorsToggle(scope) {
    if (!scope) return
    const toggles = qsa('[data-colors-toggle]', scope)
    if (!toggles.length) return
    toggles.forEach((toggle) => {
      const targetId = toggle.dataset.target || ''
      const extra = targetId ? qs(`#${targetId}`, scope) : null
      if (!extra) return
      const input = qs('input[name="color"]', scope)
      const selected = new Set(parseSelectedColorValues(input))
      const hasInExtra = qsa('.color-chip', extra).some((chip) => selected.has(chip.dataset.color || ''))
      const expanded = toggle.dataset.manualExpanded === '1' || hasInExtra
      toggle.setAttribute('aria-expanded', expanded ? 'true' : 'false')
      extra.classList.toggle('is-collapsed', !expanded)
      toggle.textContent = expanded ? 'Скрыть цвета' : 'Другие цвета'
    })
  }

  function bindOtherColorsToggle(scope) {
    if (!scope) return
    const toggles = qsa('[data-colors-toggle]', scope)
    if (!toggles.length) return
    toggles.forEach((toggle) => {
      if (toggle.dataset.bound) return
      const targetId = toggle.dataset.target || ''
      const extra = targetId ? qs(`#${targetId}`, scope) : null
      if (!extra) return
      toggle.addEventListener('click', () => {
        const expanded = toggle.getAttribute('aria-expanded') === 'true'
        toggle.dataset.manualExpanded = expanded ? '0' : '1'
        syncOtherColorsToggle(scope)
      })
      toggle.dataset.bound = '1'
    })
    syncOtherColorsToggle(scope)
  }

  function findColorChipLabel(scope, value) {
    if (!scope || !value) return value
    const chip = qsa('.color-chip', scope).find((item) => (item.dataset.color || '') === value)
    return chip?.dataset?.label || value
  }

  function removeSelectedColor(scope, value) {
    if (!scope || !value) return
    const input = qs('input[name="color"]', scope)
    const selected = parseSelectedColorValues(input)
    const updated = selected.filter((item) => item !== value)
    setSelectedColorValues(input, updated)
    syncColorChips(scope)
    bindOtherColorsToggle(scope)
  }

  function renderChoiceChips(container, items) {
    if (!container) return
    const variant = container.dataset.chipVariant || ''
    container.replaceChildren()
    const seen = new Set()
    ;(items || []).forEach((item) => {
      if (!item || !item.value) return
      const key = String(item.value || '').trim()
      if (!key || seen.has(key)) return
      seen.add(key)
      const chip = document.createElement('button')
      chip.type = 'button'
      chip.className = 'choice-chip'
      if (variant === 'swatch') chip.classList.add('choice-chip--swatch')
      if (variant === 'checkbox') chip.classList.add('choice-chip--checkbox')
      chip.dataset.value = item.value
      chip.dataset.label = item.label || item.value
      if (variant) chip.dataset.chipVariant = variant
      if (item.hex) chip.style.setProperty('--chip-color', item.hex)
      chip.textContent = item.label || item.value
      chip.setAttribute('aria-label', item.label || item.value)
      container.appendChild(chip)
    })
  }

  function readRenderedChoiceChipItems(container) {
    if (!container) return []
    const seen = new Set()
    return qsa('.choice-chip', container).reduce((acc, chip) => {
      const value = String(chip?.dataset?.value || '').trim()
      if (!value || seen.has(value)) return acc
      seen.add(value)
      const item = {
        value,
        label: String(chip?.dataset?.label || chip.textContent || value).trim() || value,
      }
      const chipColor = chip.style?.getPropertyValue?.('--chip-color') || ''
      if (chipColor) item.hex = chipColor.trim()
      acc.push(item)
      return acc
    }, [])
  }

  function syncChoiceInputOptions(scope, inputName, items, { preserveExistingOnEmpty = false } = {}) {
    if (!scope || !inputName) return
    const wrap = qs(`[data-chip-input="${inputName}"]`, scope)
    if (!wrap) return
    const nextItems = Array.isArray(items) ? items : []
    const safeItems = nextItems.length || !preserveExistingOnEmpty
      ? nextItems
      : readRenderedChoiceChipItems(wrap)
    renderChoiceChips(wrap, safeItems)
    const input = qs(`input[name="${inputName}"]`, scope)
    if (!input) return
    const allowed = new Set((safeItems || []).map((item) => item.value))
    const selected = parseSelectedCsvValues(input)
    if (!selected.length) return
    const nextSelected = selected.filter((value) => allowed.has(value))
    if (nextSelected.length !== selected.length) {
      setSelectedCsvValues(input, nextSelected)
    }
  }

  function syncChoiceChips(scope, inputName = '') {
    if (!scope) return
    const wraps = inputName
      ? qsa(`[data-chip-input="${inputName}"]`, scope)
      : qsa('[data-chip-input]', scope)
    wraps.forEach((wrap) => {
      const name = wrap.dataset.chipInput || inputName
      const input = name ? qs(`input[name="${name}"]`, scope) : null
      const selected = new Set(parseSelectedCsvValues(input))
      qsa('.choice-chip', wrap).forEach((chip) => {
        chip.classList.toggle('active', selected.has(chip.dataset.value || ''))
      })
    })
  }

  function bindChoiceChips(scope, onChange, inputName = '') {
    if (!scope) return
    const wraps = inputName
      ? qsa(`[data-chip-input="${inputName}"]`, scope)
      : qsa('[data-chip-input]', scope)
    wraps.forEach((wrap) => {
      const name = wrap.dataset.chipInput || inputName
      const input = name ? qs(`input[name="${name}"]`, scope) : null
      if (!input) return
      qsa('.choice-chip', wrap).forEach((chip) => {
        if (chip.dataset.bound) return
        chip.dataset.bound = '1'
        chip.addEventListener('click', () => {
          const next = chip.dataset.value || ''
          const selected = parseSelectedCsvValues(input)
          const hasNext = selected.includes(next)
          const updated = hasNext
            ? selected.filter((value) => value !== next)
            : [...selected, next]
          setSelectedCsvValues(input, updated)
          syncChoiceChips(scope, name)
          if (typeof onChange === 'function') onChange()
        })
      })
      syncChoiceChips(scope, name)
    })
  }

  function findChoiceChipLabel(scope, inputName, value) {
    if (!scope || !inputName || !value) return value
    const chip = qsa(`[data-chip-input="${inputName}"] .choice-chip`, scope)
      .find((item) => (item.dataset.value || '') === value)
    return chip?.dataset?.label || value
  }

  function removeSelectedChoice(scope, inputName, value) {
    if (!scope || !inputName || !value) return
    const input = qs(`input[name="${inputName}"]`, scope)
    const selected = parseSelectedCsvValues(input)
    const updated = selected.filter((item) => item !== value)
    setSelectedCsvValues(input, updated)
    syncChoiceChips(scope, inputName)
  }

  function getMultiSelectSource(scope, inputName) {
    if (!scope || !inputName) return null
    return qs(`[data-multi-source-select="${inputName}"]`, scope)
  }

  function getMultiSelectItems(select) {
    if (!select) return []
    return Array.from(select.options || [])
      .filter((opt) => String(opt.value || '').trim())
      .map((opt) => ({
        value: String(opt.value || '').trim(),
        label: String(opt.textContent || opt.value || '').trim(),
      }))
  }

  function findMultiSelectLabel(scope, inputName, value) {
    if (!scope || !inputName || !value) return value
    const select = getMultiSelectSource(scope, inputName)
    if (!select) return value
    const match = Array.from(select.options || []).find((opt) => String(opt.value || '') === String(value || ''))
    return match ? String(match.textContent || match.value || '').trim() : value
  }

  function findOverlayHost(control) {
    if (!control) return null
    const field = control.closest('.field')
    if (field) return field
    const label = control.closest('label')
    if (label) return label
    return control.closest('.search-row')
  }

  function positionFloatingOverlay(root, body, { gap = 8, minVisible = 220, maxHeight = 420, boundsEl = null } = {}) {
    if (!root || !body) return
    const rect = root.getBoundingClientRect()
    const safeGap = 12
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0
    const boundsRect = boundsEl?.getBoundingClientRect?.() || null
    const topLimit = boundsRect ? (boundsRect.top + safeGap) : safeGap
    const bottomLimit = boundsRect ? (boundsRect.bottom - safeGap) : Math.max(safeGap, viewportHeight - safeGap)
    const spaceBelow = Math.max(0, bottomLimit - rect.bottom)
    const spaceAbove = Math.max(0, rect.top - topLimit)
    const openUp = spaceBelow < minVisible && spaceAbove > spaceBelow
    const available = openUp ? spaceAbove : spaceBelow
    const appliedMaxHeight = Math.max(160, Math.min(maxHeight, Math.max(available, 160)))
    body.style.top = openUp ? 'auto' : `calc(100% + ${gap}px)`
    body.style.bottom = openUp ? `calc(100% + ${gap}px)` : 'auto'
    body.style.maxHeight = `${appliedMaxHeight}px`
  }

  function bindFloatingOverlayPosition(root, body, options = {}) {
    if (!root || !body || root.__overlayPositionBound === '1') return
    const reposition = () => {
      if (!root.open) return
      window.requestAnimationFrame(() => positionFloatingOverlay(root, body, options))
    }
    root.addEventListener('toggle', reposition)
    window.addEventListener('resize', reposition, { passive: true })
    document.addEventListener('scroll', reposition, true)
    root.__overlayPositionBound = '1'
    root.__overlayReposition = reposition
  }

  function bindMultiSelectMenus(scope, onChange, inputName = '') {
    if (!scope) return
    const selects = inputName
      ? qsa(`[data-multi-source-select="${inputName}"]`, scope)
      : qsa('[data-multi-source-select]', scope)
    selects.forEach((select) => {
      const name = select.dataset.multiSourceSelect || ''
      const input = name ? qs(`input[name="${name}"]`, scope) : null
      if (!name || !input) return
      const host = findOverlayHost(select)
      if (!host) return
      let container = host.querySelector(`[data-multi-menu-for="${name}"]`)
      if (!container) {
        container = document.createElement('div')
        container.className = 'multi-select-menu'
        container.dataset.multiMenuFor = name
        host.appendChild(container)
      }
      host.classList.add('has-multi-select-menu')
      container.classList.toggle('multi-select-menu--fuel', name === 'engine_type')
      select.hidden = true
      select.setAttribute('aria-hidden', 'true')
      select.style.setProperty('display', 'none', 'important')

      if (!container.__built) {
        const root = document.createElement('details')
        root.className = 'multi-select-menu__root'
        const summary = document.createElement('summary')
        summary.className = 'multi-select-menu__summary'
        root.appendChild(summary)
        const body = document.createElement('div')
        body.className = 'multi-select-menu__body'
        const clearBtn = document.createElement('button')
        clearBtn.type = 'button'
        clearBtn.className = 'multi-select-menu__clear'
        const optionsWrap = document.createElement('div')
        optionsWrap.className = 'multi-select-menu__options'
        const actions = document.createElement('div')
        actions.className = 'multi-select-menu__actions'
        const applyBtn = document.createElement('button')
        applyBtn.type = 'button'
        applyBtn.className = 'multi-select-menu__apply'
        applyBtn.textContent = 'Применить'
        actions.appendChild(applyBtn)
        body.appendChild(clearBtn)
        body.appendChild(optionsWrap)
        body.appendChild(actions)
        root.appendChild(body)
        container.appendChild(root)
        container.__built = true
        container.__root = root
        container.__summary = summary
        container.__clearBtn = clearBtn
        container.__optionsWrap = optionsWrap
        container.__applyBtn = applyBtn
        container.__draft = new Set(parseSelectedCsvValues(input))
        bindFloatingOverlayPosition(root, body, { maxHeight: 420, boundsEl: root.closest('.filters-panel') || null })

        const syncState = () => {
          const items = getMultiSelectItems(select)
          const allowed = new Set(items.map((item) => item.value))
          const committedValues = parseSelectedCsvValues(input).filter((value) => allowed.has(value))
          if (committedValues.join(',') !== String(input.value || '')) {
            setSelectedCsvValues(input, committedValues)
          }
          container.__draft = new Set(Array.from(container.__draft || []).filter((value) => allowed.has(value)))
          const selectedSet = root.open ? container.__draft : new Set(committedValues)
          const labelMap = new Map(items.map((item) => [item.value, item.label]))
          const placeholder = select.dataset.multiPlaceholder || 'Неважно'
          const selectedLabels = Array.from(selectedSet)
            .map((value) => labelMap.get(value) || value)
            .filter(Boolean)
          if (!selectedLabels.length) {
            summary.textContent = placeholder
          } else if (selectedLabels.length === 1) {
            summary.textContent = selectedLabels[0]
          } else if (selectedLabels.length === 2) {
            summary.textContent = selectedLabels.join(', ')
          } else {
            summary.textContent = `Выбрано: ${selectedLabels.length}`
          }
          clearBtn.textContent = 'Сбросить выбор'
          clearBtn.disabled = selectedSet.size === 0
          optionsWrap.replaceChildren()
          items.forEach((item) => {
            const btn = document.createElement('button')
            btn.type = 'button'
            btn.className = 'multi-select-menu__option'
            btn.dataset.multiValue = item.value
            btn.textContent = item.label
            btn.classList.toggle('is-active', selectedSet.has(item.value))
            btn.addEventListener('mousedown', (event) => {
              event.preventDefault()
            })
            btn.addEventListener('click', (event) => {
              event.preventDefault()
              event.stopPropagation()
              if (container.__draft.has(item.value)) {
                container.__draft.delete(item.value)
              } else {
                container.__draft.add(item.value)
              }
              syncState()
            })
            optionsWrap.appendChild(btn)
          })
          container.classList.toggle('is-hidden', items.length === 0)
          if (root.open && typeof root.__overlayReposition === 'function') {
            root.__overlayReposition()
          }
        }

        clearBtn.addEventListener('mousedown', (event) => {
          event.preventDefault()
        })
        clearBtn.addEventListener('click', (event) => {
          event.preventDefault()
          event.stopPropagation()
          if (clearBtn.disabled) return
          container.__draft = new Set()
          syncState()
        })
        applyBtn.addEventListener('mousedown', (event) => {
          event.preventDefault()
        })
        applyBtn.addEventListener('click', (event) => {
          event.preventDefault()
          event.stopPropagation()
          const nextValues = Array.from(container.__draft)
          setSelectedCsvValues(input, nextValues)
          select.value = nextValues.length === 1 ? nextValues[0] : ''
          root.open = false
          syncState()
          if (typeof onChange === 'function') onChange()
        })
        root.addEventListener('toggle', () => {
          container.__draft = new Set(parseSelectedCsvValues(input))
          syncState()
        })
        if (!container.dataset.outsideBound) {
          document.addEventListener('click', (event) => {
            if (!container.contains(event.target)) {
              root.open = false
            }
          })
          container.dataset.outsideBound = '1'
        }
        select.__multiMenuSync = syncState
      }

      if (typeof select.__multiMenuSync === 'function') {
        select.__multiMenuSync()
      }
    })
  }

  function syncMultiSelectMenus(scope, inputName = '') {
    if (!scope) return
    const selects = inputName
      ? qsa(`[data-multi-source-select="${inputName}"]`, scope)
      : qsa('[data-multi-source-select]', scope)
    selects.forEach((select) => {
      if (typeof select.__multiMenuSync === 'function') {
        select.__multiMenuSync()
      }
    })
  }

  function syncRegMonthState(scope) {
    if (!scope) return
    const pairs = [
      {
        year: qs('#reg-year-min', scope),
        month: qs('#reg-month-min', scope),
        label: qs('[data-reg-month="min"]', scope),
      },
      {
        year: qs('#reg-year-max', scope),
        month: qs('#reg-month-max', scope),
        label: qs('[data-reg-month="max"]', scope),
      },
    ]
    pairs.forEach(({ year, month, label }) => {
      if (!year || !month || !label) return
      const hasYear = Boolean(year.value) && !year.disabled
      if (!hasYear) {
        month.value = ''
      }
      month.disabled = !hasYear
    })
  }

  function bindRegMonthState(scope) {
    if (!scope) return
    const yearMin = qs('#reg-year-min', scope)
    const yearMax = qs('#reg-year-max', scope)
    yearMin?.addEventListener('change', () => syncRegMonthState(scope))
    yearMax?.addEventListener('change', () => syncRegMonthState(scope))
    syncRegMonthState(scope)
  }

  function bindRegionSelect(scope) {
    if (!scope) return
    const region = qs('[data-region-select]', scope)
    const euPanel = qs('[data-region-panel="eu"]', scope)
    const krPanel = qs('[data-region-panel="kr"]', scope)
    const countrySelect = qs('[data-country]', scope)
    const krSelect = qs('[data-kr-type]', scope)
    if (!region) return
    if (!region.value && countrySelect && countrySelect.value) {
      region.value = String(countrySelect.value).toUpperCase() === 'KR' ? 'KR' : 'EU'
    }
    const togglePanel = (panel, hidden) => {
      if (!panel) return
      const keep = panel.dataset.regionKeep === '1'
      if (keep) {
        panel.classList.toggle('is-hidden-keep', hidden)
        panel.classList.remove('is-hidden')
        return
      }
      panel.classList.toggle('is-hidden', hidden)
      panel.classList.remove('is-hidden-keep')
    }
    const update = () => {
      const val = region.value
      togglePanel(euPanel, val !== 'EU')
      togglePanel(krPanel, val !== 'KR')
      if (val === 'EU') {
        if (countrySelect) {
          countrySelect.disabled = false
        }
      } else if (val === 'KR') {
        if (countrySelect) {
          countrySelect.value = ''
          countrySelect.disabled = true
        }
      } else {
        if (countrySelect) {
          countrySelect.value = ''
          countrySelect.disabled = true
        }
      }
    }
    region.addEventListener('change', update)
    countrySelect?.addEventListener('change', () => {
      if (countrySelect.value && region.value !== 'EU') {
        region.value = 'EU'
      }
      update()
    })
    krSelect?.addEventListener('change', () => {
      if (krSelect.value && region.value !== 'KR') {
        region.value = 'KR'
      }
      update()
    })
    update()
  }

  function renderSkeleton(cards, count = 6) {
    const items = []
    for (let i = 0; i < count; i++) {
      items.push(`
        <div class="car-card skeleton-card">
          <div class="thumb-wrap skeleton-thumb"></div>
          <div class="car-card__body">
            <div>
              <div class="skeleton-line w-60"></div>
              <div class="skeleton-line w-40"></div>
              <ul class="specs">
                <li><div class="skeleton-line w-40"></div></li>
                <li><div class="skeleton-line w-30"></div></li>
                <li><div class="skeleton-line w-50"></div></li>
              </ul>
            </div>
            <div class="car-card__price skeleton-line w-30"></div>
          </div>
        </div>
      `)
    }
    cards.innerHTML = items.join('')
  }

  function renderActiveFilters(params) {
    const container = qs('#activeFilters')
    const form = qs('#filters')
    if (!container || !form) return
    const labels = {
      country: 'Страна',
      brand: 'Марка',
      model: 'Модель',
      generation: 'Поколение',
      body_type: 'Кузов',
      condition: 'Состояние',
      engine_type: 'Топливо',
      transmission: 'Трансмиссия',
      drive_type: 'Привод',
      price_min: 'Цена от',
      price_max: 'Цена до',
      mileage_min: 'Пробег от',
      mileage_max: 'Пробег до',
      power_hp_min: 'Мощность от',
      power_hp_max: 'Мощность до',
      engine_cc_min: 'Объём от',
      engine_cc_max: 'Объём до',
      num_seats: 'Мест',
      doors_count: 'Двери',
      owners_count: 'Владельцы',
      emission_class: 'Класс выбросов',
      efficiency_class: 'Эко-стикер',
      climatisation: 'Климат',
      airbags: 'Подушки',
      interior_design: 'Отделка салона',
      interior_color: 'Цвет салона',
      interior_material: 'Материал салона',
      vat_reclaimable: 'НДС',
      price_rating_label: 'Оценка цены',
      color: 'Цвет',
      region: null,
      kr_type: null,
      interior_color: 'Цвет салона',
      interior_material: 'Материал салона',
      air_suspension: 'Пневмоподвеска',
      reg_year_min: null,
      reg_month_min: null,
      reg_year_max: null,
      reg_month_max: null,
      line: null,
      sort: null,
    }
    const selectLabel = (name, value) => {
      const multiSource = getMultiSelectSource(form, name)
      if (multiSource && value) {
        return findMultiSelectLabel(form, name, value)
      }
      const el = form.elements[name]
      if (!el || !value || el.tagName !== 'SELECT') return value
      const opt = Array.from(el.options || []).find((o) => o.value === value)
      return opt ? opt.textContent.trim() : value
    }
    const countryLabel = (value) => {
      const map = window.COUNTRY_LABELS || {}
      return map[value] || value
    }
    const colorLabel = (value) => findColorChipLabel(form, value)
    const interiorLabel = (value) => findChoiceChipLabel(form, 'interior_design', value)
    const interiorColorLabel = (value) => findChoiceChipLabel(form, 'interior_color', value)
    const interiorMaterialLabel = (value) => findChoiceChipLabel(form, 'interior_material', value)
    const regMinYear = params.get('reg_year_min')
    const regMinMonth = params.get('reg_month_min')
    const regMaxYear = params.get('reg_year_max')
    const regMaxMonth = params.get('reg_month_max')
    const chips = []
    params.forEach((value, key) => {
      if (!value || ['page', 'page_size'].includes(key)) return
      // skip sort in active chips to avoid debug-look
      if (key === 'sort') return
      const hasLabel = Object.prototype.hasOwnProperty.call(labels, key)
      if (!hasLabel) return
      const label = labels[key]
      if (label === null) return
      let displayValue = value
      if (key === 'country') {
        displayValue = countryLabel(value)
      }
      if (key === 'price_min' || key === 'price_max') {
        const n = Number(value)
        displayValue = Number.isFinite(n) ? formatRub(n) : value
      }
      if (key === 'mileage_min' || key === 'mileage_max') {
        const n = Number(value)
        displayValue = Number.isFinite(n) ? `${n.toLocaleString('ru-RU')} км` : value
      }
      if (['body_type', 'engine_type', 'transmission', 'drive_type'].includes(key)) {
        parseSelectedCsvValues(value).forEach((itemValue) => {
          chips.push({
            key,
            label,
            value: selectLabel(key, itemValue),
            removeChoice: itemValue,
            removeChoiceInput: key,
          })
        })
        return
      }
      if (['condition'].includes(key)) {
        displayValue = selectLabel(key, value)
      }
      if (key === 'vat_reclaimable') {
        displayValue = value === '1' ? 'Возмещается' : value === '0' ? 'Не возмещается' : value
      }
      if (key === 'power_hp_min' || key === 'power_hp_max') {
        const n = Number(value)
        displayValue = Number.isFinite(n) ? `${n.toLocaleString('ru-RU')} л.с` : value
      }
      if (key === 'engine_cc_min' || key === 'engine_cc_max') {
        const n = Number(value)
        displayValue = Number.isFinite(n) ? `${n.toLocaleString('ru-RU')} см³` : value
      }
      if (key === 'color') {
        parseSelectedColorValues(value).forEach((colorValue) => {
          chips.push({
            key,
            label,
            value: colorLabel(colorValue),
            removeColor: colorValue,
          })
        })
        return
      }
      if (key === 'interior_design') {
        parseSelectedCsvValues(value).forEach((trimValue) => {
          chips.push({
            key,
            label,
            value: interiorLabel(trimValue),
            removeChoice: trimValue,
            removeChoiceInput: 'interior_design',
          })
        })
        return
      }
      if (key === 'interior_color') {
        parseSelectedCsvValues(value).forEach((colorValue) => {
          chips.push({
            key,
            label,
            value: interiorColorLabel(colorValue),
            removeChoice: colorValue,
            removeChoiceInput: 'interior_color',
          })
        })
        return
      }
      if (key === 'interior_material') {
        parseSelectedCsvValues(value).forEach((materialValue) => {
          chips.push({
            key,
            label,
            value: interiorMaterialLabel(materialValue),
            removeChoice: materialValue,
            removeChoiceInput: 'interior_material',
          })
        })
        return
      }
      chips.push({ key, label, value: displayValue })
    })
    if (regMinYear || regMinMonth) {
      const parts = []
      if (regMinMonth) parts.push(selectLabel('reg_month_min', regMinMonth))
      if (regMinYear) parts.push(regMinYear)
      chips.push({
        keys: ['reg_year_min', 'reg_month_min'],
        label: 'Учёт от',
        value: parts.join(' '),
      })
    }
    if (regMaxYear || regMaxMonth) {
      const parts = []
      if (regMaxMonth) parts.push(selectLabel('reg_month_max', regMaxMonth))
      if (regMaxYear) parts.push(regMaxYear)
      chips.push({
        keys: ['reg_year_max', 'reg_month_max'],
        label: 'Учёт до',
        value: parts.join(' '),
      })
    }
    if (!chips.length) {
      container.innerHTML = '<span class="muted">Фильтры не выбраны</span>'
      return
    }
    container.innerHTML = ''
    chips.forEach(({ key, keys, label, value, removeColor, removeChoice, removeChoiceInput }) => {
      const chip = document.createElement('button')
      chip.type = 'button'
      chip.className = 'filter-chip'
      const summaryOnly = Boolean(removeColor || removeChoice)
      chip.innerHTML = summaryOnly
        ? `<span>${label}: ${value}</span>`
        : `<span>${label}: ${value}</span><span class="chip-close">×</span>`
      if (removeColor) {
        chip.dataset.removeColor = removeColor
      }
      if (removeChoice) {
        chip.dataset.removeChoice = removeChoice
      }
      if (removeChoiceInput) {
        chip.dataset.removeChoiceInput = removeChoiceInput
      }
      if (summaryOnly) {
        chip.style.cursor = 'default'
        chip.setAttribute('aria-disabled', 'true')
        container.appendChild(chip)
        return
      }
      chip.addEventListener('click', () => {
        if (key === 'color' && value) {
          removeSelectedColor(form, chip.dataset.removeColor || '')
          updateCatalogUrlFromParams(collectParams(1))
          loadCars(1, { scrollToTop: true })
          return
        }
        if ((key === 'interior_design' || key === 'interior_color' || key === 'interior_material') && chip.dataset.removeChoice) {
          removeSelectedChoice(form, chip.dataset.removeChoiceInput || key, chip.dataset.removeChoice || '')
          updateCatalogUrlFromParams(collectParams(1))
          loadCars(1, { scrollToTop: true })
          return
        }
        const toClear = keys || [key]
        if (!keys && key === 'region') {
          toClear.push('country', 'kr_type')
        }
        toClear.forEach((item) => {
          const el = form.elements[item]
          if (el) el.value = ''
        })
        if (toClear.includes('model')) {
          const modelSelect = qs('#model-select')
          if (modelSelect) modelSelect.value = ''
        }
        if (toClear.includes('brand')) {
          const modelSelect = qs('#model-select')
          if (modelSelect) {
            modelSelect.innerHTML = '<option value="">Все</option>'
            modelSelect.disabled = true
          }
        }
        if (toClear.includes('color')) {
          syncColorChips(form)
          bindOtherColorsToggle(form)
        }
        if (toClear.includes('interior_design')) syncChoiceChips(form, 'interior_design')
        if (toClear.includes('interior_color')) syncChoiceChips(form, 'interior_color')
        if (toClear.includes('interior_material')) syncChoiceChips(form, 'interior_material')
        if (toClear.includes('body_type')) syncMultiSelectMenus(form, 'body_type')
        if (toClear.includes('engine_type')) syncMultiSelectMenus(form, 'engine_type')
        if (toClear.includes('transmission')) syncMultiSelectMenus(form, 'transmission')
        if (toClear.includes('drive_type')) syncMultiSelectMenus(form, 'drive_type')
        updateCatalogUrlFromParams(collectParams(1))
        loadCars(1, { scrollToTop: true })
      })
      container.appendChild(chip)
    })
  }

  function collectParams(page) {
    const form = qs('#filters')
    if (form?.id === 'filters') {
      syncCatalogLinesFromState(form)
    }
    const data = new FormData(form)
    const params = new URLSearchParams()
    const numericKeys = [
      'price_min',
      'price_max',
      'mileage_min',
      'mileage_max',
      'reg_year_min',
      'reg_month_min',
      'reg_year_max',
      'reg_month_max',
      'power_hp_min',
      'power_hp_max',
      'engine_cc_min',
      'engine_cc_max',
    ]
    for (const [k, v] of data.entries()) {
      if (!v) continue
      if (k === 'brand') {
        const norm = normalizeBrand(v)
        if (norm) params.append(k, norm)
        continue
      }
      if (numericKeys.includes(k)) {
        const n = Number(v)
        if (Number.isFinite(n)) {
          params.append(k, String(n))
        }
        continue
      }
      params.append(k, v)
    }
    // region-dependent fields
    const regionSel = form.elements['region']
    const countrySel = form.elements['country']
    const krSel = form.elements['kr_type']
    if (regionSel && regionSel.value) {
      const regionVal = String(regionSel.value).toUpperCase()
      params.set('region', regionVal)
      if (regionVal === 'EU' && countrySel && countrySel.value) {
        params.set('country', String(countrySel.value).toUpperCase())
      }
      if (regionVal === 'KR') {
        params.set('country', 'KR')
        if (krSel && krSel.value) params.set('kr_type', String(krSel.value).toUpperCase())
      }
    } else {
      if (countrySel && countrySel.value) {
        params.set('region', 'EU')
        params.set('country', String(countrySel.value).toUpperCase())
      } else if (krSel && krSel.value) {
        params.set('region', 'KR')
        params.set('country', 'KR')
        params.set('kr_type', String(krSel.value).toUpperCase())
      }
    }
    params.set('page', String(page || 1))
    params.set('page_size', '12')
    const pageField = form.querySelector('input[name="page"]')
    if (pageField) pageField.value = String(page || 1)
    return params
  }

  function normalizeParamsString(qsString) {
    if (!qsString) return ''
    const params = new URLSearchParams(qsString)
    params.delete('page')
    params.delete('page_size')
    const entries = Array.from(params.entries())
    entries.sort((a, b) => {
      if (a[0] === b[0]) return String(a[1]).localeCompare(String(b[1]))
      return a[0].localeCompare(b[0])
    })
    return entries.map(([k, v]) => `${k}=${v}`).join('&')
  }

  function renderEmpty(cards, reason = 'Нет результатов по выбранным фильтрам.') {
    cards.innerHTML = `<div class="empty-state">${reason}<br><button class="btn btn-secondary" id="emptyReset">Сбросить фильтры</button></div>`
    qs('#emptyReset')?.addEventListener('click', () => {
      const form = qs('#filters')
      form?.reset()
      clearCatalogModelLineInputs(form)
      syncColorChips(form)
      syncChoiceChips(form)
      syncMultiSelectMenus(form)
      syncRegMonthState(form)
      bindOtherColorsToggle(form)
      const modelSelect = qs('#model-select')
      if (modelSelect) {
        modelSelect.innerHTML = '<option value=\"\">Все</option>'
        modelSelect.disabled = true
        setAccordionSelectedModels(modelSelect, [])
      }
      loadCars(1, { scrollToTop: true })
    })
  }

  // -------- favorites --------
  const favoriteIds = new Set()

  const isAuthed = document.body?.dataset?.auth === '1'

  async function loadFavoritesState() {
    if (!isAuthed) return
    try {
      const res = await fetch('/api/favorites')
      if (!res.ok) return
      const data = await res.json()
      favoriteIds.clear()
      ;(data.ids || []).forEach((id) => favoriteIds.add(Number(id)))
      syncFavoriteButtons()
    } catch (e) {
      console.warn('favorites', e)
    }
  }

  function syncFavoriteButtons(root = document) {
    root.querySelectorAll('[data-fav-button]').forEach((btn) => {
      const id = Number(btn.dataset.carId)
      const active = favoriteIds.has(id)
      btn.classList.toggle('active', active)
    })
  }

  async function toggleFavorite(btn) {
    if (!isAuthed) {
      alert('Войдите, чтобы сохранять избранное')
      return
    }
    const carId = Number(btn.dataset.carId)
    if (!carId) return
    const isActive = favoriteIds.has(carId)
    const method = isActive ? 'DELETE' : 'POST'
    try {
      const res = await fetch(`/api/favorites/${carId}`, { method })
      if (res.status === 401) {
        alert('Войдите, чтобы сохранять избранное')
        return
      }
      if (!res.ok) throw new Error('fav failed')
      if (isActive) favoriteIds.delete(carId)
      else favoriteIds.add(carId)
      syncFavoriteButtons()
    } catch (e) {
      console.error(e)
    }
  }

  function bindFavoriteButtons(root = document) {
    root.querySelectorAll('[data-fav-button]').forEach((btn) => {
      btn.removeEventListener('click', btn.__favHandler)
      btn.__favHandler = (e) => {
        e.preventDefault()
        e.stopPropagation()
        toggleFavorite(btn)
      }
      btn.addEventListener('click', btn.__favHandler)
    })
    syncFavoriteButtons(root)
  }
  // -------- end favorites --------

  let catalogController = null
  let catalogReqId = 0

  function scrollCatalogToTop() {
    const anchor = qs('.results-header') || qs('.catalog__content') || qs('#cards')
    if (!anchor) return
    const prefersReduce = window.matchMedia('(prefers-reduced-motion: reduce)')
    const top = Math.max(0, Math.round(anchor.getBoundingClientRect().top + window.scrollY - 12))
    window.scrollTo({
      top,
      behavior: prefersReduce.matches ? 'auto' : 'smooth',
    })
  }

  function renderCatalogMeta(page, pageSize, total) {
    const pageInfo = qs('#pageInfo')
    const resultCount = qs('#resultCount')
    const pageNumbers = qs('#pageNumbers')
    const safePageSize = Math.max(1, Number(pageSize) || 12)
    const safePage = Math.max(1, Number(page) || 1)
    const safeTotal = Math.max(0, Number(total) || 0)
    const totalPages = Math.max(1, Math.ceil(safeTotal / safePageSize))

    if (pageInfo) {
      pageInfo.textContent = `Страница ${safePage} из ${totalPages}`
    }

    if (resultCount) {
      if (safeTotal === 0) {
        resultCount.textContent = 'Ничего не найдено. Измените фильтры.'
      } else {
        const from = (safePage - 1) * safePageSize + 1
        const to = Math.min(safeTotal, safePage * safePageSize)
        resultCount.textContent = `Показано ${from}-${to} из ${safeTotal}`
      }
    }

    if (!pageNumbers) return
    pageNumbers.innerHTML = ''
    const addBtn = (targetPage, label = null) => {
      const button = document.createElement('button')
      button.className = 'btn page-btn' + (targetPage === safePage ? ' active' : '')
      button.textContent = label || String(targetPage)
      button.addEventListener('click', () => loadCars(targetPage, { scrollToTop: true }))
      pageNumbers.appendChild(button)
    }
    addBtn(1)
    const windowSize = 5
    const start = Math.max(2, safePage - 2)
    const end = Math.min(totalPages - 1, start + windowSize - 1)
    if (start > 2) {
      const dots = document.createElement('span')
      dots.className = 'page-dots'
      dots.textContent = '…'
      pageNumbers.appendChild(dots)
    }
    for (let p = start; p <= end; p += 1) addBtn(p)
    if (end < totalPages - 1) {
      const dots = document.createElement('span')
      dots.className = 'page-dots'
      dots.textContent = '…'
      pageNumbers.appendChild(dots)
    }
    if (totalPages > 1) addBtn(totalPages)
  }

  async function loadCars(page = 1, options = {}) {
    const { scrollToTop = false } = options || {}
    const spinner = qs('#spinner')
    const cards = qs('#cards')
    if (!spinner || !cards) return
    const paramsPreview = collectParams(page)
    if (DEBUG_FILTERS) console.info('filters:catalog loadCars', paramsPreview.toString())
    const ssrEnabled = cards.dataset.ssr === '1'
    const ssrParams = cards.dataset.ssrParams || ''
    const hasSSR = ssrEnabled && cards.querySelector('.car-card')
    const reuseSSR = page === 1 && hasSSR
      && normalizeParamsString(ssrParams) === normalizeParamsString(paramsPreview.toString())
    spinner.style.display = 'block'
    if (!reuseSSR) {
      renderSkeleton(cards)
    }
    if (scrollToTop) {
      requestAnimationFrame(scrollCatalogToTop)
    }
    try {
      const params = paramsPreview
      const reqId = ++catalogReqId
      catalogController?.abort()
      catalogController = new AbortController()
      const res = await fetch(`${window.CATALOG_API}?${params.toString()}`, { signal: catalogController.signal })
      if (reqId !== catalogReqId) return
      if (!res.ok) {
        throw new Error(`API ${res.status}`)
      }
      const data = await res.json()
      if (reqId !== catalogReqId) return
      renderActiveFilters(params)
      renderCatalogMeta(data.page, data.page_size, data.total)

      if (!reuseSSR) {
        cards.innerHTML = ''
      }
      if (!Array.isArray(data.items) || !data.items.length) {
        renderEmpty(cards)
        return
      }

      if (reuseSSR) {
        const itemsById = new Map()
        data.items.forEach((it) => {
          if (it && it.id != null) itemsById.set(String(it.id), it)
        })
        qsa('#cards .car-card').forEach((card) => {
          const id = card.getAttribute('data-id')
          if (!id || !itemsById.has(id)) return
          const item = itemsById.get(id)
          const img = card.querySelector('img.thumb')
          if (img) {
            const rawThumb = item.thumbnail_url || ''
            const src = normalizeThumbUrl(rawThumb, { thumb: true })
            const orig = normalizeThumbUrl(rawThumb)
            img.dataset.thumb = src
            img.setAttribute('src', src)
            img.dataset.orig = (orig && orig !== '/static/img/no-photo.svg') ? orig : ''
            applyThumbFallback(img)
          }
          const titleNode = card.querySelector('.car-card__title')
          if (titleNode) {
            titleNode.textContent = `${item.brand || ''} ${item.model || ''}`.trim()
          }
          let subtitleNode = card.querySelector('.car-card__subtitle')
          if (item.variant) {
            if (!subtitleNode && titleNode?.parentNode) {
              subtitleNode = document.createElement('div')
              subtitleNode.className = 'car-card__subtitle'
              titleNode.insertAdjacentElement('afterend', subtitleNode)
            }
            if (subtitleNode) subtitleNode.textContent = item.variant
          } else if (subtitleNode) {
            subtitleNode.remove()
            subtitleNode = null
          }
          let regLabel = ''
          if (item.registration_year) {
            const m = Number(item.registration_month || 1)
            const label = window.MONTH_LABELS && window.MONTH_LABELS[m]
            regLabel = label ? `${label} ${item.registration_year}` : `${String(m).padStart(2, '0')}.${item.registration_year}`
          } else if (item.year) {
            regLabel = String(item.year)
          }
          const metaText = [regLabel, item.display_engine_type || item.engine_type].filter(Boolean).join(' · ')
          let metaNode = card.querySelector('.car-card__meta')
          if (metaText) {
            if (!metaNode && titleNode?.parentNode) {
              metaNode = document.createElement('div')
              metaNode.className = 'car-card__meta'
              ;(subtitleNode || titleNode).insertAdjacentElement('afterend', metaNode)
            }
            if (metaNode) metaNode.textContent = metaText
          } else if (metaNode) {
            metaNode.remove()
            metaNode = null
          }
          const colorDot = (hex, raw) => {
            if (!hex) return ''
            const title = raw ? ` title="${escapeHtml(raw)}"` : ''
            return `<span class="spec-dot" style="background:${hex}"${title}></span>`
          }
          const specLines = []
          if (item.mileage != null) {
            specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/mileage.svg" alt="">${Number(item.mileage).toLocaleString('ru-RU')} км</span>`)
          }
          if (item.engine_type) {
            specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/fuel.svg" alt="">${escapeHtml(item.display_engine_type || item.engine_type)}</span>`)
          }
          if (item.power_hp) {
            specLines.push(`<span class="spec-line">Мощность: ${Math.round(item.power_hp)} л.с.</span>`)
          }
          if (item.engine_cc) {
            specLines.push(`<span class="spec-line">Объём: ${Number(item.engine_cc).toLocaleString('ru-RU')} см³</span>`)
          }
          if (item.display_transmission || item.transmission) {
            specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/drive.svg" alt="">${escapeHtml(item.display_transmission || item.transmission)}</span>`)
          }
          if (item.display_body_type || item.body_type) {
            specLines.push(`<span class="spec-line">${escapeHtml(item.display_body_type || item.body_type)}</span>`)
          }
          if (item.display_color || item.color) {
            const label = escapeHtml(item.display_color || item.color)
            specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/color.svg" alt="">${colorDot(item.color_hex, item.color)}${label}</span>`)
          }
          if (item.display_country_label || item.country) {
            specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/flag.svg" alt="">${escapeHtml(item.display_country_label || item.country)}</span>`)
          }
          let specsNode = card.querySelector('.specs')
          if (specLines.length) {
            if (!specsNode && titleNode?.parentNode) {
              specsNode = document.createElement('ul')
              specsNode.className = 'specs'
              const anchorNode = metaNode || subtitleNode || titleNode
              anchorNode.insertAdjacentElement('afterend', specsNode)
            }
            if (specsNode) {
              specsNode.innerHTML = specLines.map((line) => `<li>${line}</li>`).join('')
            }
          } else if (specsNode) {
            specsNode.remove()
          }
          const priceMain = card.querySelector('.price-main')
          if (priceMain) {
            const displayRub = item.display_price_rub
            const priceText = displayRub != null ? formatRub(displayRub) : '—'
            priceMain.textContent = priceText
          }
          const priceNote = card.querySelector('.price-note')
          if (item.price_note) {
            if (priceNote) {
              priceNote.textContent = item.price_note
            } else {
              const priceWrap = card.querySelector('.car-card__price')
              if (priceWrap) {
                const note = document.createElement('div')
                note.className = 'price-note'
                note.textContent = item.price_note
                priceWrap.appendChild(note)
              }
            }
          } else if (priceNote) {
            priceNote.remove()
          }
        })
        bindFavoriteButtons(cards)
        window.__page = data.page
        window.__pageSize = data.page_size
        window.__total = data.total
        cards.dataset.ssr = '0'
        if (scrollToTop) requestAnimationFrame(scrollCatalogToTop)
        return
      }

      for (const car of data.items) {
        const card = document.createElement('a')
        card.href = `/car/${car.id}`
        card.className = 'car-card'
        const images = Array.isArray(car.images) && car.images.length ? car.images : (car.thumbnail_url ? [car.thumbnail_url] : [])
        const rawThumb = images[0] || ''
        const thumbSrc = normalizeThumbUrl(rawThumb, { thumb: true })
        const origThumb = normalizeThumbUrl(rawThumb)
        const hasGallery = images.length > 1
        const navControls = hasGallery
          ? `
            <button class="thumb-nav thumb-nav--prev" type="button" data-thumb-prev aria-label="Предыдущее фото">‹</button>
            <button class="thumb-nav thumb-nav--next" type="button" data-thumb-next aria-label="Следующее фото">›</button>
          `
          : ''
        const photosCount = car.photos_count ?? car.images_count
        const more = (photosCount && photosCount > 1 && car.thumbnail_url) ? `<span class="more-badge">+${photosCount - 1} фото</span>` : ''
        const displayRub = car.display_price_rub
        let priceText = displayRub != null ? formatRub(displayRub) : ''
        if (!priceText) priceText = '—'
        const calcLine = `<div class="price-main">${priceText}</div>`
        const priceLines = []
        const footnote = car.price_note ? `<div class="price-note">${escapeHtml(car.price_note)}</div>` : ''
        const variantLine = car.variant ? `<div class="car-card__subtitle">${escapeHtml(car.variant)}</div>` : ''
        let regLabel = ''
        if (car.registration_year) {
          const m = Number(car.registration_month || 1)
          const label = window.MONTH_LABELS && window.MONTH_LABELS[m]
          regLabel = label ? `${label} ${car.registration_year}` : `${String(m).padStart(2, '0')}.${car.registration_year}`
        } else if (car.year) {
          regLabel = String(car.year)
        }
        const metaLine = [regLabel, car.display_engine_type || car.engine_type].filter(Boolean).join(' · ')
        const colorDot = (hex, raw) => {
          if (!hex) return ''
          const title = raw ? ` title="${escapeHtml(raw)}"` : ''
          return `<span class="spec-dot" style="background:${hex}"${title}></span>`
        }
        const specLines = []
        if (car.mileage != null) {
          specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/mileage.svg" alt="">${Number(car.mileage).toLocaleString('ru-RU')} км</span>`)
        }
        if (car.engine_type) {
          specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/fuel.svg" alt="">${car.display_engine_type || car.engine_type}</span>`)
        }
        if (car.power_hp) {
          specLines.push(`<span class="spec-line">Мощность: ${Math.round(car.power_hp)} л.с.</span>`)
        }
        if (car.engine_cc) {
          specLines.push(`<span class="spec-line">Объём: ${Number(car.engine_cc).toLocaleString('ru-RU')} см³</span>`)
        }
        if (car.display_transmission || car.transmission) {
          specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/drive.svg" alt="">${car.display_transmission || car.transmission}</span>`)
        }
        if (car.display_body_type || car.body_type) {
          specLines.push(`<span class="spec-line">${car.display_body_type || car.body_type}</span>`)
        }
        if (car.display_color || car.color) {
          const label = car.display_color || car.color
          specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/color.svg" alt="">${colorDot(car.color_hex, car.color)}${label}</span>`)
        }
        if (car.display_country_label || car.country) {
          specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/flag.svg" alt="">${car.display_country_label || car.country}</span>`)
        }
        card.innerHTML = `
          <div class="thumb-wrap">
            <img
              class="thumb"
              src="${thumbSrc}"
              srcset="${thumbSrc} 1x"
              sizes="(max-width: 768px) 50vw, 320px"
              alt=""
              loading="lazy"
              decoding="async"
              referrerpolicy="no-referrer"
              data-thumb="${thumbSrc}"
              data-orig="${origThumb}"
              data-id="${car.id}"
              onerror="this.onerror=null;this.src='/static/img/no-photo.svg';"
              fetchpriority="low"
              width="320"
              height="200"
            />
            ${navControls}
            ${more}
            <button class="fav-btn" data-fav-button data-car-id="${car.id}" aria-label="Добавить в избранное">★</button>
          </div>
          <div class="car-card__body">
            <div>
              <div class="car-card__title">${car.brand || ''} ${car.model || ''}</div>
              ${variantLine}
              ${metaLine ? `<div class="car-card__meta">${metaLine}</div>` : ''}
              ${specLines.length ? `<ul class="specs">${specLines.map((s) => `<li>${s}</li>`).join('')}</ul>` : ''}
            </div>
            <div class="car-card__price">
              ${calcLine}
              ${priceLines.map((p) => `<div class="price-sub">${p}</div>`).join('')}
              ${footnote}
            </div>
          </div>
        `
        const img = card.querySelector('img.thumb')
        const wrap = card.querySelector('.thumb-wrap')
        if (wrap) wrap.classList.remove('thumb-loading')
        if (img) {
          const finalize = () => {
            img.style.opacity = '1'
            if (wrap) wrap.classList.remove('thumb-loading')
          }
          if (img.complete && img.naturalWidth > 0) {
            finalize()
          } else {
            img.addEventListener('load', finalize)
            img.addEventListener('error', finalize)
            requestAnimationFrame(finalize)
          }
        }
        if (hasGallery && img) {
          let index = 0
          const updateThumb = () => {
          let nextSrc = normalizeThumbUrl(images[index] || '', { thumb: true })
          if (!(nextSrc.startsWith('https://') || nextSrc.startsWith('/'))) {
            nextSrc = '/static/img/no-photo.svg'
          }
          img.src = nextSrc
          img.srcset = `${nextSrc} 1x`
          }
          const onNav = (delta) => {
            index = (index + delta + images.length) % images.length
            updateThumb()
          }
          const prev = card.querySelector('[data-thumb-prev]')
          const next = card.querySelector('[data-thumb-next]')
          const bindNav = (btn, delta) => {
            if (!btn) return
            btn.addEventListener('click', (e) => {
              e.preventDefault()
              e.stopPropagation()
              onNav(delta)
            })
          }
          bindNav(prev, -1)
          bindNav(next, 1)
        }
        cards.appendChild(card)
      }
      bindFavoriteButtons(cards)
      window.__page = data.page
      window.__pageSize = data.page_size
      window.__total = data.total
      if (scrollToTop) requestAnimationFrame(scrollCatalogToTop)
    } catch (e) {
      if (e?.name === 'AbortError') return
      console.error(e)
      if (pageInfo) pageInfo.textContent = 'Ошибка загрузки'
      if (cards) renderEmpty(cards, 'Не удалось загрузить данные. Попробуйте позже.')
    } finally {
      spinner.style.display = 'none'
    }
  }

  function initCatalog() {
    if (!qs('#cards')) return
    const saved = sessionStorage.getItem('catalogScroll')
    if (saved) {
      requestAnimationFrame(() => {
        window.scrollTo({ top: Number(saved) || 0, behavior: 'auto' })
      })
      sessionStorage.removeItem('catalogScroll')
    }
    const filtersForm = qs('#filters')
    const selectedFilters = parseSelectedFilters()
    if (DEBUG_FILTERS) {
      console.info('catalog:init selected', selectedFilters.toString())
    }
    if (filtersForm) syncFormFromSelected(filtersForm, selectedFilters)
    qsa('#cards img.thumb').forEach((img) => {
      applyThumbFallback(img)
    })
    const urlParams = new URLSearchParams(window.location.search)
    const initialPage = Number(urlParams.get('page') || 1)
    const initialModelParam = urlParams.get('model') || ''
    let initialModelRestorePending = Boolean(initialModelParam)
    const initialSort = urlParams.get('sort') || 'price_asc'
    const modelSelect = qs('#model-select')
    const brandSelect = qs('#brand')
    const generationSelect = qs('#generation')
    const generationField = generationSelect ? generationSelect.closest('[data-generation-field]') : null
    const advancedLink = qs('#catalog-advanced-link')
    const cards = qs('#cards')
    normalizeBrandOptions(brandSelect)
    let initialReapplyDone = false
    const initialLineModels = selectedFilters
      .getAll('line')
      .map((line) => parseLineValue(line))
      .filter((item) => item.brand || item.model)
    const getInitialLineModelsForBrand = (brand) => {
      const normalizedBrand = normalizeBrand(brand || '')
      return initialLineModels
        .filter((item) => item.model && (!normalizedBrand || !item.brand || item.brand === normalizedBrand))
        .map((item) => item.model)
    }
    const reapplySelected = () => {
      if (!filtersForm) return
      selectedFilters.forEach((value, key) => {
        if (!value || key === 'line') return
        const field = filtersForm.elements[key]
        if (!field) return
        const nextValue = key === 'brand' ? normalizeBrand(value) : value
        if (field.tagName === 'SELECT') {
          ensureOption(field, nextValue)
          setSelectValueInsensitive(field, nextValue)
        } else {
          field.value = nextValue
        }
      })
      if (DEBUG_FILTERS) console.info('catalog: reapplySelected source=initial')
    }

    const syncGenerationVisibility = () => {
      if (!generationSelect || !generationField) return
      const generationValues = Array.from(generationSelect.options || []).filter((opt) => opt.value)
      const hasGenerations = generationValues.length > 0
      generationField.classList.toggle('is-hidden', !hasGenerations)
      generationSelect.disabled = !hasGenerations
      if (!hasGenerations) generationSelect.value = ''
    }

    const loadCatalogFilterBase = async () => {
      if (!filtersForm) return
      if (loadCatalogFilterBase.__pending) return loadCatalogFilterBase.__pending
      loadCatalogFilterBase.__pending = (async () => {
      try {
        const params = new URLSearchParams()
        const region = qs('#region')?.value || selectedFilters.get('region') || ''
        const country = qs('#country')?.value || selectedFilters.get('country') || ''
        if (region) params.set('region', region)
        if (region === 'EU' && country) params.set('country', country)
        if (DEBUG_FILTERS) console.info('catalog: region changed -> fetching ctx', params.toString())
        const res = await fetch(`/api/filter_ctx_base?${params.toString()}`)
        if (!res.ok) return
        const data = await res.json()
        setSelectOptions(qs('#brand'), data.brands || [], { emptyLabel: 'Все', labelKey: 'label', valueKey: 'value' })
        normalizeBrandOptions(qs('#brand'))
        setSelectOptions(qs('#body_type'), data.body_types || [], { emptyLabel: 'Любой' })
        setSelectOptions(qs('[data-multi-source-select="engine_type"]', filtersForm), data.engine_types || [], { emptyLabel: 'Любое' })
        setSelectOptions(qs('[data-multi-source-select="transmission"]', filtersForm), data.transmissions || [], { emptyLabel: 'Любая' })
        setSelectOptions(qs('[data-multi-source-select="drive_type"]', filtersForm), data.drive_types || [], { emptyLabel: 'Любой' })
        const preserveChoiceChips = filtersForm.dataset.baseHydrated !== '1'
        syncChoiceInputOptions(filtersForm, 'interior_color', data.interior_color_options || [], { preserveExistingOnEmpty: preserveChoiceChips })
        syncChoiceInputOptions(filtersForm, 'interior_material', data.interior_material_options || [], { preserveExistingOnEmpty: preserveChoiceChips })
        setSelectOptions(qs('#reg-year-min'), data.reg_years || [], { emptyLabel: 'Не важно', labelKey: 'value', valueKey: 'value' })
        setSelectOptions(qs('#reg-year-max'), data.reg_years || [], { emptyLabel: 'Не важно', labelKey: 'value', valueKey: 'value' })
        const countrySelect = qs('#country')
        setSelectOptions(countrySelect, data.countries || [], { emptyLabel: 'Все страны', labelKey: 'label', valueKey: 'value' })
        const regionSelectEl = qs('#region')
        if (regionSelectEl && Array.isArray(data.regions) && data.regions.length) {
          setSelectOptions(regionSelectEl, data.regions, { emptyLabel: 'Все регионы', labelKey: 'label', valueKey: 'value' })
        }
        const basic = qs('.color-swatches--basic')
        const extra = qs('#colors-extra-catalog')
        if (basic && ((data.colors_basic || []).length || !basic.children.length)) {
          renderColorChips(basic, data.colors_basic || [])
        }
        if (extra && ((data.colors_other || []).length || !extra.children.length)) {
          renderColorChips(extra, data.colors_other || [])
        }
        if (DEBUG_FILTERS) {
          console.info('catalog: ctx loaded countries=' + (data.countries || []).length + ' kr_types=' + (data.kr_types || []).length)
        }
        filtersForm.dataset.baseHydrated = '1'
        bindColorChips(filtersForm, () => loadCars(1, { scrollToTop: true }))
        bindChoiceChips(filtersForm, () => loadCars(1, { scrollToTop: true }))
        bindMultiSelectMenus(filtersForm, () => loadCars(1, { scrollToTop: true }))
        bindOtherColorsToggle(filtersForm)
        syncColorChips(filtersForm)
        syncChoiceChips(filtersForm)
        syncMultiSelectMenus(filtersForm)
      } catch (e) {
        console.warn('filters base', e)
      } finally {
        loadCatalogFilterBase.__pending = null
      }
      })()
      return loadCatalogFilterBase.__pending
    }

    const hydrateCatalogFromSSR = () => {
      if (!cards || cards.dataset.ssr !== '1' || !cards.querySelector('.car-card')) return false
      const total = Number(cards.dataset.ssrTotal || '0')
      const page = Number(cards.dataset.ssrPage || String(initialPage || 1))
      const pageSize = Number(cards.dataset.ssrPageSize || '12')
      if (!total || !pageSize) return false
      const params = collectParams(page)
      renderActiveFilters(params)
      renderCatalogMeta(page, pageSize, total)
      window.__page = page
      window.__pageSize = pageSize
      window.__total = total
      return true
    }

    if (DEBUG_FILTERS) {
      const debugCount = sessionStorage.getItem('homeCountParams')
      const debugSubmit = sessionStorage.getItem('homeSubmitParams')
      if (debugCount || debugSubmit) {
        const countParams = new URLSearchParams(debugCount || '')
        const submitParams = new URLSearchParams(debugSubmit || '')
        const catalogParams = collectParams(initialPage)
        catalogParams.delete('page')
        catalogParams.delete('page_size')
        console.info('filters:count', countParams.toString())
        console.info('filters:submit', submitParams.toString())
        console.info('filters:catalog', catalogParams.toString())
        console.info('filters:diff count vs submit', diffParams(countParams, submitParams))
        console.info('filters:diff submit vs catalog', diffParams(submitParams, catalogParams))
        sessionStorage.removeItem('homeCountParams')
        sessionStorage.removeItem('homeSubmitParams')
      }
    }

    qs('#resetFilters')?.addEventListener('click', (e) => {
      e.preventDefault()
      const form = qs('#filters')
      form?.reset()
      clearCatalogModelLineInputs(form)
      if (modelSelect) setAccordionSelectedModels(modelSelect, [])
      syncColorChips(form)
      syncChoiceChips(form)
      syncMultiSelectMenus(form)
      syncRegMonthState(form)
      bindOtherColorsToggle(form)
      bindRegionSelect(form)
      if (modelSelect) {
        fillModelSelectWithGroups(modelSelect, { models: [], model_groups: [] }, 'Все')
        modelSelect.disabled = true
      }
      sessionStorage.setItem('catalogScroll', String(0))
      loadCars(1, { scrollToTop: true })
    })

    qs('#prevPage')?.addEventListener('click', () => {
      const p = Math.max(1, (window.__page || 1) - 1)
      loadCars(p, { scrollToTop: true })
    })
    qs('#nextPage')?.addEventListener('click', () => {
      const max = Math.max(1, Math.ceil((window.__total || 0) / (window.__pageSize || 12)))
      const p = Math.min(max, (window.__page || 1) + 1)
      loadCars(p, { scrollToTop: true })
    })

    if (filtersForm) {
      bindColorChips(filtersForm, () => loadCars(1, { scrollToTop: true }))
      bindChoiceChips(filtersForm, () => loadCars(1, { scrollToTop: true }))
      bindMultiSelectMenus(filtersForm, () => loadCars(1, { scrollToTop: true }))
      bindOtherColorsToggle(filtersForm)
      syncChoiceChips(filtersForm)
      syncMultiSelectMenus(filtersForm)
      bindRegMonthState(filtersForm)
      bindRegionSelect(filtersForm)
      const ctrls = qsa('input, select', filtersForm)
      let debounce
      const updateAdvancedLink = () => {
        if (!advancedLink) return
        const params = collectParams(1)
        params.delete('page')
        params.delete('page_size')
        const qs = params.toString()
        advancedLink.href = qs ? `/search?${qs}` : '/search'
      }
      const trigger = (event) => {
        const sourceEl = event?.currentTarget
        if (sourceEl?.dataset?.skipTriggerOnce === '1') {
          sourceEl.dataset.skipTriggerOnce = ''
          return
        }
        clearTimeout(debounce)
        debounce = setTimeout(() => {
          const params = collectParams(1)
          updateCatalogUrlFromParams(params)
          if (DEBUG_FILTERS) console.info('catalog: loadCars params', params.toString())
          loadCars(1, { scrollToTop: true })
          updateAdvancedLink()
        }, 250)
      }
      ctrls.forEach((el) => {
        el.addEventListener('change', trigger)
        el.addEventListener('input', trigger)
      })
      const logChange = (label, el) => {
        if (!el) return
        let prev = el.value
        el.addEventListener('change', () => {
          const next = el.value
          if (DEBUG_FILTERS) console.info(`catalog: ${label} change ${prev} -> ${next} source=ui`)
          prev = next
        })
      }
      logChange('region', qs('#region'))
      logChange('country', qs('#country'))
      logChange('kr_type', qs('#kr-type'))
      const handleCatalogLocationChange = async () => {
        clearCatalogModelLineInputs(filtersForm)
        if (modelSelect) {
          modelSelect.value = ''
          setAccordionSelectedModels(modelSelect, [])
        }
        await loadCatalogFilterBase()
        if (brandSelect?.value) {
          await updateCatalogModels()
          return
        }
        if (modelSelect) {
          fillModelSelectWithGroups(modelSelect, { models: [], model_groups: [] }, 'Все')
          modelSelect.disabled = true
          modelSelect.__modelAccordionSync?.()
        }
        if (generationSelect) {
          setSelectOptions(generationSelect, [], { emptyLabel: 'Любое' })
          syncGenerationVisibility()
        }
      }
      qs('#region')?.addEventListener('change', handleCatalogLocationChange)
      qs('#country')?.addEventListener('change', handleCatalogLocationChange)
      qs('#kr-type')?.addEventListener('change', handleCatalogLocationChange)
      const sortSelect = qs('#sortHidden', filtersForm)
      if (sortSelect && initialSort) sortSelect.value = initialSort
      const sortTopbar = qs('#sort-select')
      if (sortTopbar && initialSort) sortTopbar.value = initialSort
      if (sortTopbar) {
        sortTopbar.addEventListener('change', () => {
          const val = sortTopbar.value
          if (sortSelect) sortSelect.value = val
          loadCars(1, { scrollToTop: true })
          updateAdvancedLink()
        })
      }
      updateAdvancedLink()
    }

    const toggle = qs('#filtersToggle')
    const panel = qs('#filtersPanel')
    const overlay = qs('#filtersOverlay')
    const closeBtn = qs('#filtersClose')
    const setFiltersOpen = (next) => {
      if (!panel) return
      panel.classList.toggle('open', next)
      overlay?.classList.toggle('open', next)
      document.body.classList.toggle('filters-open', next)
    }
    toggle?.addEventListener('click', () => setFiltersOpen(true))
    closeBtn?.addEventListener('click', () => setFiltersOpen(false))
    overlay?.addEventListener('click', () => setFiltersOpen(false))
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') setFiltersOpen(false)
    })

    async function updateCatalogModels() {
      if (!brandSelect || !modelSelect) return
      const brand = brandSelect.value
      const normBrand = normalizeBrand(brand)
      const catalogForm = modelSelect.form
      modelSelect.innerHTML = ''
      if (!normBrand) {
        clearCatalogModelLineInputs(catalogForm)
        setAccordionSelectedModels(modelSelect, [])
        fillModelSelectWithGroups(modelSelect, { models: [], model_groups: [] }, 'Все')
        modelSelect.disabled = true
        if (generationSelect) {
          setSelectOptions(generationSelect, [], { emptyLabel: 'Любое' })
          syncGenerationVisibility()
        }
        return
      }
      modelSelect.disabled = true
      modelSelect.innerHTML = '<option value="">Загрузка…</option>'
      const region = qs('[name="region"]')?.value || ''
      const country = qs('[name="country"]')?.value || ''
      const krType = qs('[name="kr_type"]')?.value || ''
      const payload = await fetchModels({ brand: normBrand, region, country, krType })
      fillModelSelectWithGroups(modelSelect, payload, 'Все')
      const selectedLines = getCatalogSelectedModels(catalogForm, normBrand)
      const restoredModelsRaw = selectedLines.length ? selectedLines : getInitialLineModelsForBrand(normBrand)
      const restoredModels = typeof modelSelect.__resolveModelValues === 'function'
        ? modelSelect.__resolveModelValues(restoredModelsRaw)
        : restoredModelsRaw
      if (restoredModels.length > 1) {
        modelSelect.value = ''
        setAccordionSelectedModels(modelSelect, restoredModels)
        syncCatalogLinesFromState(catalogForm)
      } else if (restoredModels.length === 1) {
        setSelectValueInsensitive(modelSelect, restoredModels[0])
        setAccordionSelectedModels(modelSelect, [restoredModels[0]])
        clearCatalogModelLineInputs(catalogForm)
      } else if (initialModelRestorePending && initialModelParam) {
        const restoredInitial = typeof modelSelect.__resolveModelValues === 'function'
          ? modelSelect.__resolveModelValues([initialModelParam])
          : [initialModelParam]
        const initialValue = restoredInitial[0] || initialModelParam
        setSelectValueInsensitive(modelSelect, initialValue)
        setAccordionSelectedModels(modelSelect, [initialValue])
      } else {
        setAccordionSelectedModels(modelSelect, [])
      }
      initialModelRestorePending = false
      modelSelect.disabled = false
      modelSelect.__modelAccordionSync?.()
      if (generationSelect) {
        if (modelSelect.value) {
          modelSelect.dataset.skipTriggerOnce = '1'
          modelSelect.dispatchEvent(new Event('change', { bubbles: true }))
        } else {
          setSelectOptions(generationSelect, [], { emptyLabel: 'Любое' })
          syncGenerationVisibility()
        }
      }
    }
    brandSelect?.addEventListener('change', () => {
      clearCatalogModelLineInputs(filtersForm)
      if (modelSelect) {
        modelSelect.value = ''
        setAccordionSelectedModels(modelSelect, [])
      }
      updateCatalogModels()
    })
    modelSelect?.addEventListener('change', async () => {
      if (!generationSelect) return
      const params = new URLSearchParams()
      const region = qs('[name="region"]')?.value || ''
      const country = qs('[name="country"]')?.value || ''
      if (region) params.set('region', region)
      if (region === 'EU' && country) params.set('country', country)
      if (brandSelect?.value) params.set('brand', normalizeBrand(brandSelect.value))
      if (modelSelect.value) params.set('model', modelSelect.value)
      if (DEBUG_FILTERS) console.info('catalog: model ctx', params.toString())
      try {
        const res = await fetch(`/api/filter_ctx_model?${params.toString()}`)
        if (!res.ok) return
        const data = await res.json()
        setSelectOptions(generationSelect, data.generations || [], { emptyLabel: 'Любое', labelKey: 'label', valueKey: 'value' })
        syncGenerationVisibility()
      } catch (e) {
        console.warn('filter model', e)
      }
    })
    const loadInitial = async () => {
      const baseHydrated = filtersForm?.dataset.baseHydrated === '1'
      const basePromise = baseHydrated ? Promise.resolve() : loadCatalogFilterBase()
      if (!initialReapplyDone) {
        reapplySelected()
        initialReapplyDone = true
      }
      const ssrHydrated = hydrateCatalogFromSSR()
      if (!ssrHydrated) {
        void loadCars(initialPage)
      }
      if (brandSelect && brandSelect.value) {
        await basePromise
        await updateCatalogModels()
      }
      syncGenerationVisibility()
      if (!baseHydrated) {
        void basePromise
      }
    }
    loadInitial()
  }
  function initNav() {
    const burgers = qsa('.header__burger')
    const drawer = qs('#headerDrawer')
    const closeBtn = qs('.header__drawer-close')
    const links = qsa('#headerDrawer a')
    const profileToggle = qs('#profileToggle')
    const profileMenu = qs('#profileMenu')
    const toggleDrawer = () => {
      if (!drawer) return
      const next = !drawer.classList.contains('open')
      drawer.classList.toggle('open', next)
      document.body.classList.toggle('drawer-open', next)
    }
    burgers.forEach((b) => b.addEventListener('click', toggleDrawer))
    closeBtn?.addEventListener('click', () => {
      drawer?.classList.remove('open')
      document.body.classList.remove('drawer-open')
    })
    drawer?.addEventListener('click', (e) => {
      if (e.target === drawer) {
        drawer.classList.remove('open')
        document.body.classList.remove('drawer-open')
      }
    })
    links.forEach((l) =>
      l.addEventListener('click', () => {
        drawer?.classList.remove('open')
        document.body.classList.remove('drawer-open')
      }),
    )

    const closeProfile = () => {
      if (profileMenu && profileMenu.classList.contains('open')) {
        profileMenu.classList.remove('open')
        if (profileToggle) profileToggle.setAttribute('aria-expanded', 'false')
      }
    }
    profileToggle?.addEventListener('click', (e) => {
      e.stopPropagation()
      if (!profileMenu) return
      const next = !profileMenu.classList.contains('open')
      profileMenu.classList.toggle('open', next)
      profileToggle.setAttribute('aria-expanded', next ? 'true' : 'false')
    })
    document.addEventListener('click', (e) => {
      if (!profileMenu || !profileToggle) return
      if (profileMenu.contains(e.target)) return
      if (profileToggle.contains(e.target)) return
      closeProfile()
    })
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        closeProfile()
        drawer?.classList.remove('open')
        document.body.classList.remove('drawer-open')
      }
    })

    bindFavoriteButtons()
  }

  function applyLeadPrefill() {
    const form = qs('#lead-form')
    if (!form) return
    const dataStr = localStorage.getItem('lead_prefill')
    if (!dataStr) return
    try {
      const data = JSON.parse(dataStr)
      if (data.preferred) {
        const preferred = qs('#lead-preferred')
        if (preferred && !preferred.value) preferred.value = data.preferred
      }
      if (data.comment) {
        const comment = qs('#lead-comment')
        if (comment && !comment.value) comment.value = data.comment
      }
      if (data.price_range) {
        const price = qs('#lead-price')
        if (price && !price.value) price.value = data.price_range
      }
    } catch (e) {
      console.warn('lead prefill parse', e)
    } finally {
      localStorage.removeItem('lead_prefill')
    }
  }

  function initLeadFromDetail() {
    const cta = qs('[data-lead-cta]')
    if (!cta) return
    cta.addEventListener('click', (e) => {
      e.preventDefault()
      const title = cta.dataset.carTitle || ''
      const price = cta.dataset.carPrice || ''
      const link = cta.dataset.carLink || window.location.href
      const payload = {
        preferred: title,
        price_range: price,
        comment: `Хочу эту машину: ${title}${price ? ' · ' + price : ''}\nСсылка: ${link}`,
      }
      // user data (if present in data-* attributes)
      if (cta.dataset.userName) payload.name = cta.dataset.userName
      if (cta.dataset.userPhone) payload.phone = cta.dataset.userPhone
      if (cta.dataset.userEmail) payload.email = cta.dataset.userEmail
      localStorage.setItem('lead_prefill', JSON.stringify(payload))
      window.location.href = '/#lead-form'
    })
  }

  async function fetchBrands() {
    try {
      const res = await fetch('/api/brands')
      return await res.json()
    } catch (e) {
      console.error('brands', e)
      return []
    }
  }

  async function fetchModels({ brand, region, country, krType } = {}) {
    if (!brand) return { models: [], model_groups: [] }
    try {
      const params = new URLSearchParams()
      if (region) params.set('region', region)
      if (country) params.set('country', country)
      if (krType) params.set('kr_type', krType)
      params.set('brand', normalizeBrand(brand))
      const res = await fetch(`/api/filter_ctx_brand?${params.toString()}`)
      if (!res.ok) return { models: [], model_groups: [] }
      const data = await res.json()
      return {
        models: data.models || [],
        model_groups: data.model_groups || [],
      }
    } catch (e) {
      console.error('models', e)
      return { models: [], model_groups: [] }
    }
  }

  async function fetchCatalogModels({ region, country, brand }) {
    if (!brand) return { models: [], model_groups: [] }
    try {
      const params = new URLSearchParams()
      if (region) params.set('region', region)
      if (country) params.set('country', country)
      params.set('brand', normalizeBrand(brand))
      const res = await fetch(`/api/filter_ctx_brand?${params.toString()}`)
      if (!res.ok) return { models: [], model_groups: [] }
      const data = await res.json()
      return {
        models: data.models || [],
        model_groups: data.model_groups || [],
      }
    } catch (e) {
      console.warn('models ctx', e)
      return { models: [], model_groups: [] }
    }
  }

  function fillModelSelectWithGroups(select, payload, emptyLabel = 'Все') {
    if (!select) return
    const models = Array.isArray(payload?.models) ? payload.models : []
    const groups = Array.isArray(payload?.model_groups) ? payload.model_groups : []
    const enableAccordion = window.ENABLE_MODEL_ACCORDION === true
      const modelMeta = new Map()
      models.forEach((row) => {
        const value = String(row?.value || row?.model || '').trim()
        if (!value) return
        const aliases = Array.from(
        new Set(
          (Array.isArray(row?.aliases) ? row.aliases : [])
            .map((item) => String(item || '').trim())
            .filter(Boolean)
            .concat(value),
        ),
      )
      modelMeta.set(value, {
        value,
        label: row?.label || value,
        aliases,
        baseModel: String(row?.base_model || '').trim(),
      })
    })
    select.__modelMeta = modelMeta
    select.__resolveModelValues = (values = []) => {
      const resolved = []
      const seen = new Set()
      ;(Array.isArray(values) ? values : [values]).forEach((item) => {
        const raw = String(item || '').trim()
        if (!raw) return
        const matchedValues = []
        for (const [value, meta] of modelMeta.entries()) {
          if (
            value === raw
            || (meta.aliases || []).includes(raw)
            || (meta.baseModel && meta.baseModel === raw)
          ) {
            matchedValues.push(value)
          }
        }
        if (!matchedValues.length) {
          if (seen.has(raw)) return
          seen.add(raw)
          resolved.push(raw)
          return
        }
        matchedValues.forEach((matched) => {
          if (seen.has(matched)) return
          seen.add(matched)
          resolved.push(matched)
        })
      })
      return resolved
    }
    select.innerHTML = ''
    const empty = document.createElement('option')
    empty.value = ''
    empty.textContent = emptyLabel
    select.appendChild(empty)

    const appendOption = (row) => {
      const value = row?.value || row?.model || row
      if (!value) return null
      const opt = document.createElement('option')
      opt.value = value
      opt.textContent = row?.label || value
      return opt
    }

    const isCatalogModelSelect = select?.form?.id === 'filters' && (select?.name === 'model' || select?.id === 'model-select')
    const syncGeneratedModelLines = (selectedModels) => {
      if (!isCatalogModelSelect || !select?.form) return
      clearCatalogModelLineInputs(select.form)
      const brandField = select.form.elements['brand']
      const brand = normalizeBrand(brandField?.value || '')
      const values = Array.isArray(selectedModels) ? selectedModels.filter(Boolean) : []
      if (!brand || values.length <= 1) return
      setCatalogModelLineInputs(
        select.form,
        values.map((modelValue) => `${brand}|${modelValue}|`),
      )
    }
    const getGeneratedModelValues = () => {
      if (!isCatalogModelSelect || !select?.form) return []
      const brandField = select.form.elements['brand']
      return getCatalogSelectedModels(select.form, brandField?.value || '')
    }

    const removeAccordion = () => {
      const host = findOverlayHost(select)
      const key = select.id || select.name || 'model'
      const container = host?.querySelector?.(`[data-model-accordion-for="${key}"]`)
      if (container) container.remove()
      if (select.__modelAccordionChangeHandler) {
        select.removeEventListener('change', select.__modelAccordionChangeHandler)
        select.__modelAccordionChangeHandler = null
      }
      select.__modelAccordionSync = null
      if (host) host.classList.remove('has-model-accordion')
      select.classList.remove('model-select-native')
      select.hidden = false
      select.removeAttribute('aria-hidden')
      select.style.removeProperty('display')
      clearCatalogModelLineInputs(select.form)
    }

    const setAccordionState = (container, selectedValue, selectedValues = []) => {
      if (!container) return
      const selectedSet = new Set((selectedValues || []).map((v) => String(v || '')))
      qsa('[data-model-value]', container).forEach((btn) => {
        const value = String(btn.dataset.modelValue || '')
        const active = selectedSet.size
          ? selectedSet.has(value)
          : value === String(selectedValue || '')
        btn.classList.toggle('is-active', active)
      })
      const selectedEl = qs('[data-model-selected]', container)
      if (selectedEl) {
        if (selectedSet.size > 1) {
          selectedEl.textContent = `Выбрано моделей: ${selectedSet.size}`
        } else {
          const selectedOpt = Array.from(select.options || []).find((o) => String(o.value) === String(selectedValue || ''))
          selectedEl.textContent = selectedOpt?.textContent || emptyLabel
        }
      }
      qsa('[data-model-group-values]', container).forEach((btn) => {
        let groupValues = []
        try {
          groupValues = JSON.parse(btn.dataset.modelGroupValues || '[]')
        } catch {
          groupValues = []
        }
        const matchedCount = groupValues.filter((value) => selectedSet.has(String(value || ''))).length
        const isActive = Boolean(groupValues.length) && matchedCount === groupValues.length
        const isPartial = matchedCount > 0 && matchedCount < groupValues.length
        btn.classList.toggle('is-active', isActive)
        btn.classList.toggle('is-partial', isPartial)
        const groupEl = btn.closest('.model-accordion__group')
        if (groupEl) {
          groupEl.classList.toggle('is-active', isActive)
          groupEl.classList.toggle('is-partial', isPartial)
          if (matchedCount > 0) groupEl.open = true
        }
      })
    }

    const renderAccordion = () => {
      if (!enableAccordion) {
        removeAccordion()
        return
      }
      const host = findOverlayHost(select)
      if (!host) return
      let container = host.querySelector(`[data-model-accordion-for="${select.id || select.name || 'model'}"]`)
      if (!container) {
        container = document.createElement('div')
        container.className = 'model-accordion'
        container.dataset.modelAccordionFor = select.id || select.name || 'model'
        host.appendChild(container)
      }
      host.classList.add('has-model-accordion')
      if (!groups.length) {
        if (select.__modelAccordionChangeHandler) {
          select.removeEventListener('change', select.__modelAccordionChangeHandler)
          select.__modelAccordionChangeHandler = null
        }
        select.__modelAccordionSync = null
        container.innerHTML = ''
        container.classList.add('is-hidden')
        host.classList.remove('has-model-accordion')
        select.classList.remove('model-select-native')
        select.hidden = false
        select.removeAttribute('aria-hidden')
        select.style.removeProperty('display')
        return
      }

      select.classList.add('model-select-native')
      select.hidden = true
      select.setAttribute('aria-hidden', 'true')
      select.style.setProperty('display', 'none', 'important')
      container.classList.remove('is-hidden')
      container.innerHTML = ''

      const root = document.createElement('details')
      root.className = 'model-accordion__root'
      const rootSummary = document.createElement('summary')
      rootSummary.textContent = 'Серии и модели'
      root.appendChild(rootSummary)
      const rootBody = document.createElement('div')
      rootBody.className = 'model-accordion__body'
      const contentWrap = document.createElement('div')
      contentWrap.className = 'model-accordion__content'
      rootBody.appendChild(contentWrap)
      root.appendChild(rootBody)
      container.appendChild(root)
      bindFloatingOverlayPosition(root, rootBody, { maxHeight: 440, boundsEl: root.closest('.filters-panel') || null })
      if (!container.dataset.outsideBound) {
        document.addEventListener('click', (event) => {
          if (!container.contains(event.target)) {
            const opened = container.querySelector('.model-accordion__root[open]')
            if (opened) opened.open = false
          }
        })
        container.dataset.outsideBound = '1'
      }

      const clearBtn = document.createElement('button')
      clearBtn.type = 'button'
      clearBtn.className = 'model-accordion__clear'
      clearBtn.textContent = emptyLabel
      const applyBtn = document.createElement('button')
      applyBtn.type = 'button'
      applyBtn.className = 'model-accordion__apply'
      applyBtn.textContent = 'Применить'
      const actionsWrap = document.createElement('div')
      actionsWrap.className = 'model-accordion__actions'
      const initialSelectedValues = isCatalogModelSelect ? getGeneratedModelValues() : getAccordionSelectedModels(select)
      const selectedModels = new Set(initialSelectedValues)
      if (!selectedModels.size && select.value) selectedModels.add(select.value)
      const draftSelectedModels = new Set(selectedModels)
      setAccordionSelectedModels(select, Array.from(selectedModels))
      const syncDraftState = () => {
        setAccordionState(container, select.value || '', Array.from(draftSelectedModels))
        if (root.open && typeof root.__overlayReposition === 'function') {
          root.__overlayReposition()
        }
      }
      const resetDraftSelection = () => {
        draftSelectedModels.clear()
        selectedModels.forEach((value) => draftSelectedModels.add(value))
        syncDraftState()
      }
      const applySelection = () => {
        selectedModels.clear()
        draftSelectedModels.forEach((value) => selectedModels.add(value))
        const values = Array.from(selectedModels)
        setAccordionSelectedModels(select, values)
        if (values.length === 1) {
          select.value = values[0]
        } else {
          select.value = ''
        }
        syncGeneratedModelLines(values)
        setAccordionState(container, select.value || '', values)
        root.open = false
        select.dispatchEvent(new Event('change', { bubbles: true }))
      }
      const toggleModelSelection = (value) => {
        if (!value) return
        if (draftSelectedModels.has(value)) {
          draftSelectedModels.delete(value)
        } else {
          draftSelectedModels.add(value)
        }
        syncDraftState()
      }
      clearBtn.addEventListener('mousedown', (event) => {
        event.preventDefault()
      })
      clearBtn.addEventListener('click', (event) => {
        event.preventDefault()
        event.stopPropagation()
        draftSelectedModels.clear()
        syncDraftState()
        applySelection()
      })
      contentWrap.appendChild(clearBtn)

      groups.forEach((group) => {
        const groupModels = Array.isArray(group?.models) ? group.models : []
        const count = Number(group?.count || 0)
        if (groupModels.length === 1) {
          const row = groupModels[0]
          const value = row?.value || row?.model || ''
          if (!value) return
          const itemBtn = document.createElement('button')
          itemBtn.type = 'button'
          itemBtn.className = 'model-accordion__item'
          itemBtn.dataset.modelValue = value
          itemBtn.textContent = `${group?.label || row?.label || value}${count ? ` (${count})` : ''}`
          itemBtn.addEventListener('mousedown', (event) => {
            event.preventDefault()
          })
          itemBtn.addEventListener('click', (event) => {
            event.preventDefault()
            event.stopPropagation()
            toggleModelSelection(value)
          })
          contentWrap.appendChild(itemBtn)
          return
        }

        const details = document.createElement('details')
        details.className = 'model-accordion__group'
        const summary = document.createElement('summary')
        summary.textContent = `${group?.label || 'Прочее'}${count ? ` (${count})` : ''}`
        details.appendChild(summary)

        const modelsWrap = document.createElement('div')
        modelsWrap.className = 'model-accordion__models'
        if (groupModels.length) {
          const allBtn = document.createElement('button')
          allBtn.type = 'button'
          allBtn.className = 'model-accordion__model model-accordion__model--all'
          allBtn.textContent = 'Все в серии'
          const groupValues = groupModels
            .map((row) => row?.value || row?.model || '')
            .filter(Boolean)
          allBtn.dataset.modelGroupValues = JSON.stringify(groupValues)
          allBtn.addEventListener('mousedown', (event) => {
            event.preventDefault()
          })
          allBtn.addEventListener('click', (event) => {
            event.preventDefault()
            event.stopPropagation()
            const shouldClearGroup = groupValues.every((value) => draftSelectedModels.has(value))
            groupValues.forEach((value) => {
              if (shouldClearGroup) {
                draftSelectedModels.delete(value)
              } else {
                draftSelectedModels.add(value)
              }
            })
            syncDraftState()
          })
          modelsWrap.appendChild(allBtn)
        }
        groupModels.forEach((row) => {
          const value = row?.value || row?.model || ''
          if (!value) return
          const btn = document.createElement('button')
          btn.type = 'button'
          btn.className = 'model-accordion__model'
          btn.dataset.modelValue = value
          btn.textContent = row?.label || value
          btn.addEventListener('mousedown', (event) => {
            event.preventDefault()
          })
          btn.addEventListener('click', (event) => {
            event.preventDefault()
            event.stopPropagation()
            toggleModelSelection(value)
          })
          modelsWrap.appendChild(btn)
        })
        details.appendChild(modelsWrap)
        contentWrap.appendChild(details)
      })

      actionsWrap.appendChild(applyBtn)
      rootBody.appendChild(actionsWrap)
      applyBtn.addEventListener('mousedown', (event) => {
        event.preventDefault()
      })
      applyBtn.addEventListener('click', (event) => {
        event.preventDefault()
        event.stopPropagation()
        applySelection()
      })
      root.addEventListener('toggle', () => {
        if (!root.open) {
          resetDraftSelection()
        } else {
          resetDraftSelection()
        }
      })

      setAccordionState(container, select.value || '', Array.from(selectedModels))
      if (select.__modelAccordionChangeHandler) {
        select.removeEventListener('change', select.__modelAccordionChangeHandler)
      }
      const handleModelAccordionChange = () => {
        selectedModels.clear()
        const lineValues = getGeneratedModelValues()
        const storedValues = !isCatalogModelSelect ? getAccordionSelectedModels(select) : []
        if (lineValues.length) {
          lineValues.forEach((value) => selectedModels.add(value))
        } else if (storedValues.length > 1) {
          storedValues.forEach((value) => selectedModels.add(value))
        } else if (select.value) {
          selectedModels.add(select.value)
          syncGeneratedModelLines([select.value])
        } else {
          syncGeneratedModelLines([])
        }
        setAccordionSelectedModels(select, Array.from(selectedModels))
        resetDraftSelection()
      }
      select.addEventListener('change', handleModelAccordionChange)
      select.__modelAccordionChangeHandler = handleModelAccordionChange
      select.__modelAccordionSync = handleModelAccordionChange
    }

    if (groups.length) {
      groups.forEach((group) => {
        const groupModels = Array.isArray(group?.models) ? group.models : []
        if (!groupModels.length) return
        const og = document.createElement('optgroup')
        og.label = group?.label || 'Прочее'
        groupModels.forEach((row) => {
          const opt = appendOption(row)
          if (opt) og.appendChild(opt)
        })
        if (og.children.length) select.appendChild(og)
      })
      renderAccordion()
      return
    }

    models.forEach((row) => {
      const opt = appendOption(row)
      if (opt) select.appendChild(opt)
    })
    renderAccordion()
  }

  function setSelectValueInsensitive(select, value) {
    if (!select || !value) return false
    const target = String(value).trim().toLowerCase()
    if (!target) return false
    const options = Array.from(select.options || [])
    const match = options.find((opt) => {
      const optVal = String(opt.value || '').toLowerCase()
      const optText = String(opt.textContent || '').toLowerCase()
      return optVal === target || optText === target
    })
    if (match) {
      select.value = match.value
      return true
    }
    return false
  }

  function initHome() {
    const form = qs('#home-search')
    if (!form) return
    const resetBtn = qs('#home-reset')
    const brandSelect = qs('#home-brand')
    const modelSelect = qs('#home-model')
    const submitBtn = qs('#home-submit')
    const countEl = qs('#home-count')
    const badgeCountEl = qs('#home-badge-count')
    const countEls = qsa('[data-home-count]')
    const regionSelect = qs('#home-region')
    const regionSlot = qs('#home-region-slot')
    const regionSlotSelect = qs('#home-region-slot-select')
    const regionSlotLabel = qs('#home-region-slot-label')
    const advancedLink = qs('#home-advanced-link')
    const homeCountries = (window.HOME_COUNTRIES || []).slice().sort((a, b) => {
      const av = String(a?.label || a?.value || a || '').toLowerCase()
      const bv = String(b?.label || b?.value || b || '').toLowerCase()
      return av.localeCompare(bv, 'ru')
    })
    normalizeBrandOptions(brandSelect)
    let initialAnimation = true
    let pendingController = null

    const getHomeSelectedModels = () => {
      const selected = getAccordionSelectedModels(modelSelect)
      if (selected.length) return selected
      const fallback = String(modelSelect?.value || '').trim()
      return fallback ? [fallback] : []
    }

    if (resetBtn) {
      resetBtn.addEventListener('click', (e) => {
        e.preventDefault()
        form.reset()
        bindRegionSelect(form)
        if (modelSelect) {
          modelSelect.innerHTML = '<option value="">Все</option>'
          modelSelect.disabled = true
          setAccordionSelectedModels(modelSelect, [])
          modelSelect.__modelAccordionSync?.()
        }
        initialAnimation = true
        updateCount()
      })
    }

    async function updateHomeModels(selectedOverride = null) {
      if (!brandSelect || !modelSelect) return
      const brand = normalizeBrand(brandSelect.value)
      const selectedValues = Array.isArray(selectedOverride)
        ? Array.from(new Set(selectedOverride.map((value) => String(value || '').trim()).filter(Boolean)))
        : getHomeSelectedModels()
      modelSelect.innerHTML = ''
      if (!brand) {
        modelSelect.disabled = true
        modelSelect.innerHTML = '<option value="">Все</option>'
        fillModelSelectWithGroups(modelSelect, { models: [], model_groups: [] }, 'Все')
        setAccordionSelectedModels(modelSelect, [])
        modelSelect.__modelAccordionSync?.()
        return
      }
      modelSelect.disabled = true
      modelSelect.innerHTML = '<option value="">Загрузка…</option>'
      const region = regionSelect?.value || ''
      const country = regionSlotSelect?.name === 'country' ? (regionSlotSelect?.value || '') : ''
      const krType = regionSlotSelect?.name === 'kr_type' ? (regionSlotSelect?.value || '') : ''
      const payload = await fetchModels({ brand, region, country, krType })
      fillModelSelectWithGroups(modelSelect, payload, 'Все')
      const availableValues = new Set(
        Array.from(modelSelect.options || [])
          .map((opt) => String(opt.value || '').trim())
          .filter(Boolean),
      )
      const preservedValues = selectedValues.filter((value) => availableValues.has(value))
      if (preservedValues.length > 1) {
        modelSelect.value = ''
        setAccordionSelectedModels(modelSelect, preservedValues)
      } else if (preservedValues.length === 1) {
        if (setSelectValueInsensitive(modelSelect, preservedValues[0])) {
          setAccordionSelectedModels(modelSelect, preservedValues)
        } else {
          modelSelect.value = ''
          setAccordionSelectedModels(modelSelect, [])
        }
      } else {
        modelSelect.value = ''
        setAccordionSelectedModels(modelSelect, [])
      }
      modelSelect.disabled = false
      modelSelect.__modelAccordionSync?.()
    }

    brandSelect?.addEventListener('change', () => {
      updateHomeModels([]).then(() => updateCount())
    })

    function buildHomeParams(withPaging = false) {
      const data = new FormData(form)
      const params = new URLSearchParams()
      const numericKeys = ['price_max', 'mileage_max', 'reg_year_min', 'reg_month_min', 'reg_year_max', 'reg_month_max']
      const skipKeys = ['region_extra', 'model']
      let regionVal = ''
      for (const [k, v] of data.entries()) {
        if (k === 'region') regionVal = v
        if (!v) continue
        if (skipKeys.includes(k)) continue
        if (k === 'brand') {
          const norm = normalizeBrand(v)
          if (norm) {
            params.append(k, norm)
          }
          continue
        }
        if (numericKeys.includes(k)) {
          const n = Number(v)
          if (Number.isFinite(n)) {
            params.append(k, String(n))
          }
          continue
        }
        params.append(k, v)
      }
      const brand = normalizeBrand(brandSelect?.value || '')
      const selectedModels = getHomeSelectedModels()
      if (brand) {
        params.set('brand', brand)
      }
      if (brand && selectedModels.length) {
        selectedModels.forEach((modelValue) => {
          params.append('line', `${brand}|${String(modelValue || '').trim()}|`)
        })
        if (selectedModels.length === 1) {
          params.set('model', selectedModels[0])
        } else {
          params.delete('model')
        }
      } else {
        params.delete('line')
      }
      if (regionVal === 'KR') {
        params.set('country', 'KR')
        const slotVal = regionSlotSelect?.value || ''
        if (slotVal === 'KR_INTERNAL') params.set('kr_type', 'KR_INTERNAL')
        else if (slotVal === 'KR_IMPORT') params.set('kr_type', 'KR_IMPORT')
        else params.delete('kr_type')
      }
      if (withPaging) {
        params.set('page', '1')
        params.set('page_size', '1')
      }
      return params
    }

    const animateCount = (el, target) => {
      const start = Number(el.dataset.count || 0)
      const end = Number(target || 0)
      const duration = 400
      const startTs = performance.now()
      const step = (ts) => {
        const t = Math.min(1, (ts - startTs) / duration)
        const val = Math.round(start + (end - start) * t)
        el.textContent = val.toLocaleString('ru-RU')
        if (t < 1) requestAnimationFrame(step)
        else el.dataset.count = String(end)
      }
      requestAnimationFrame(step)
    }

    let debounce
    const updateCount = () => {
      if (!countEl) return
      clearTimeout(debounce)
      debounce = setTimeout(async () => {
        pendingController?.abort()
        pendingController = new AbortController()
        try {
          const params = buildHomeParams(false)
          if (DEBUG_FILTERS) console.info('home: count request', params.toString())
          const res = await fetch(`/api/cars_count?${params.toString()}`, { signal: pendingController.signal })
          if (!res.ok) return
          const data = await res.json()
          const total = Number(data.count || 0)
          if (initialAnimation) {
            countEl.dataset.count = '0'
            countEl.textContent = '0'
            if (badgeCountEl) {
              badgeCountEl.dataset.count = '0'
              badgeCountEl.textContent = '0'
            }
          }
          animateCount(countEl, total)
          if (badgeCountEl) animateCount(badgeCountEl, total)
          if (countEls.length) {
            countEls.forEach((el) => {
              if (el === countEl || el === badgeCountEl) return
              animateCount(el, total)
            })
          }
          initialAnimation = false
          updateAdvancedLink()
          if (DEBUG_FILTERS) {
            sessionStorage.setItem('homeCountParams', params.toString())
          }
        } catch (e) {
          if (e?.name === 'AbortError') return
          console.warn('home count', e)
        }
      }, 250)
    }

    function updateAdvancedLink() {
      if (!advancedLink) return
      const params = buildHomeParams(false)
      const qs = params.toString()
      advancedLink.href = qs ? `/search?${qs}` : '/search'
    }

    function updateRegionSlot() {
      const val = regionSelect?.value || ''
      if (!regionSlot || !regionSlotSelect || !regionSlotLabel) return
      regionSlotSelect.disabled = false
      regionSlotSelect.innerHTML = ''
      if (val === 'EU') {
        regionSlotLabel.textContent = 'Страна (Европа)'
        const optAll = document.createElement('option')
        optAll.value = ''
        optAll.textContent = 'Все страны'
        regionSlotSelect.appendChild(optAll)
        homeCountries.forEach((c) => {
          const opt = document.createElement('option')
          const val = c.value || c
          opt.value = val
          opt.textContent = c.label || val
          regionSlotSelect.appendChild(opt)
        })
        regionSlotSelect.name = 'country'
      } else if (val === 'KR') {
        regionSlotLabel.textContent = 'Корея (тип)'
        const optAny = document.createElement('option')
        optAny.value = ''
        optAny.textContent = 'Любой'
        regionSlotSelect.appendChild(optAny)
        const optInt = document.createElement('option')
        optInt.value = 'KR_INTERNAL'
        optInt.textContent = 'Корея (внутренний рынок)'
        regionSlotSelect.appendChild(optInt)
        const optImp = document.createElement('option')
        optImp.value = 'KR_IMPORT'
        optImp.textContent = 'Корея (импорт)'
        regionSlotSelect.appendChild(optImp)
        regionSlotSelect.name = 'kr_type'
      } else {
        regionSlotLabel.textContent = 'Страна / Тип'
        const opt = document.createElement('option')
        opt.value = ''
        opt.textContent = '—'
        regionSlotSelect.appendChild(opt)
        regionSlotSelect.removeAttribute('name')
        regionSlotSelect.disabled = true
      }
      if (DEBUG_FILTERS) console.info('home: region changed -> fetching ctx', val)
      if (DEBUG_FILTERS) console.info('home: ctx loaded countries=' + homeCountries.length + ' kr_types=' + (val == 'KR' ? 2 : 0))
      updateAdvancedLink()
    }

    const ctrls = qsa('input, select', form)
    ctrls.forEach((el) => {
      const tag = el.tagName.toLowerCase()
      if (tag === 'select') {
        el.addEventListener('change', updateCount)
      } else {
        el.addEventListener('input', updateCount)
      }
    })
    regionSelect?.addEventListener('change', () => {
      updateRegionSlot()
      updateHomeModels().then(() => updateCount())
    })
    regionSlotSelect?.addEventListener('change', () => {
      updateHomeModels().then(() => updateCount())
    })
    form.addEventListener('submit', (event) => {
      event.preventDefault()
      const params = buildHomeParams(false)
      if (DEBUG_FILTERS) {
        console.info('home: submit', params.toString())
        sessionStorage.setItem('homeSubmitParams', params.toString())
      }
      window.location.assign(buildCatalogUrl(params))
    })
    window.addEventListener('pageshow', () => {
      initialAnimation = true
      updateCount()
    })
    if (regionSelect && regionSelect.value) {
      updateRegionSlot()
      const params = new URLSearchParams(window.location.search)
      const euVal = params.get('country')
      const krVal = params.get('kr_type')
      if (regionSlotSelect) {
        if (euVal) regionSlotSelect.value = euVal
        if (krVal) regionSlotSelect.value = krVal
      }
    } else {
      updateRegionSlot()
    }
    updateCount()
    updateAdvancedLink()
  }
  function initAdvancedSearch() {
    const form = qs('#advanced-search-form')
    if (!form) return
    if (form.dataset.initialized === '1') return
    form.dataset.initialized = '1'
    const rowsWrap = qs('#search-rows')
    const template = qs('#search-row-template')
    const addBtn = qs('#add-search-row')
    const countEl = qs('#advanced-count')
    const messageEl = qs('#advanced-message')
    const suggestionsEl = qs('#advanced-suggestions')
    const regionSelect = qs('[data-region-select]', form)
    const regionSubEu = qs('[data-region-sub-eu]', form)
    const regionSubKr = qs('[data-region-sub-kr]', form)
    const regionEuSelect = qs('[data-country]', form)
    const regionKrSelect = qs('[data-kr-type]', form)
    const advancedGenerationSelect = qs('select[name="generation"]', form)
    const advancedGenerationField = advancedGenerationSelect ? advancedGenerationSelect.closest('.field') : null

    const parseOptions = (raw) => {
      try {
        const data = JSON.parse(raw || '[]')
        return Array.isArray(data) ? data : []
      } catch {
        return []
      }
    }

    const readCurrentSelectOptions = (select) => {
      if (!select) return []
      const seen = new Set()
      return Array.from(select.options || []).reduce((acc, option) => {
        const value = String(option?.value || '').trim()
        if (!value || seen.has(value)) return acc
        seen.add(value)
        acc.push({
          value,
          label: String(option?.textContent || value).trim() || value,
        })
        return acc
      }, [])
    }

    const setRegionSelectOptions = (select, options) => {
      if (!select) return
      const current = select.value
      const deduped = []
      const seen = new Set()
      options.forEach((item) => {
        const isObj = item && typeof item === 'object'
        const value = isObj ? (item.value ?? item.id ?? '') : item
        if (value == null || value === '') return
        const key = String(value)
        if (seen.has(key)) return
        seen.add(key)
        deduped.push({
          value: key,
          label: isObj ? String(item.label ?? value) : key
        })
      })
      select.innerHTML = ''
      const emptyOpt = document.createElement('option')
      emptyOpt.value = ''
      emptyOpt.textContent = 'Не важно'
      select.appendChild(emptyOpt)
      deduped.forEach((item) => {
        const opt = document.createElement('option')
        opt.value = item.value
        opt.textContent = item.label
        select.appendChild(opt)
      })
      if (current && deduped.some((item) => item.value === current)) {
        select.value = current
      } else {
        select.value = ''
      }
      select.disabled = deduped.length === 0
    }

    const payloadMap = {
      num_seats: 'seats_options',
      doors_count: 'doors_options',
      owners_count: 'owners_options',
      emission_class: 'emission_classes',
      efficiency_class: 'efficiency_classes',
      climatisation: 'climatisation_options',
      airbags: 'airbags_options',
      price_rating_label: 'price_rating_labels',
    }

    const rebuildColorChipGroup = (wrap, items) => {
      if (!wrap) return
      wrap.replaceChildren()
      const seen = new Set()
      ;(items || []).forEach((item) => {
        if (!item || !item.value) return
        const key = String(item.value || '').trim()
        if (!key || seen.has(key)) return
        seen.add(key)
        const chip = document.createElement('button')
        chip.type = 'button'
        chip.className = 'color-chip'
        chip.dataset.color = item.value
        chip.dataset.label = item.label || item.value
        chip.title = item.label || item.value
        chip.setAttribute('aria-label', item.label || item.value)
        if (item.hex) chip.style.setProperty('--chip-color', item.hex)
        chip.textContent = item.label || item.value
        wrap.appendChild(chip)
      })
    }

    const applyBasicOptions = (data) => {
      if (!data) return false
      const payloadHydrated = form.dataset.payloadHydrated === '1'
      let hadLivePayload = false
      const fallbackOptions = (items, fallback = []) => {
        const next = Array.isArray(items) ? items : []
        if (next.length) {
          hadLivePayload = true
          return next
        }
        return payloadHydrated ? next : fallback
      }
      const bodyTypeSelect = qs('[data-multi-source-select="body_type"]', form)
      const generationSelect = qs('select[name="generation"]', form)
      const engineTypeSelect = qs('[data-multi-source-select="engine_type"]', form)
      const transmissionSelect = qs('[data-multi-source-select="transmission"]', form)
      const driveTypeSelect = qs('[data-multi-source-select="drive_type"]', form)
      const regYearMin = qs('#reg-year-min', form)
      const regYearMax = qs('#reg-year-max', form)

      setSelectOptions(regionEuSelect, fallbackOptions(data.countries, readCurrentSelectOptions(regionEuSelect)), { emptyLabel: 'Все страны' })
      setSelectOptions(bodyTypeSelect, fallbackOptions(data.body_types, readCurrentSelectOptions(bodyTypeSelect)), { emptyLabel: 'Любой' })
      setSelectOptions(generationSelect, fallbackOptions(data.generations, readCurrentSelectOptions(generationSelect)), { emptyLabel: 'Любое' })
      setSelectOptions(engineTypeSelect, fallbackOptions(data.engine_types, readCurrentSelectOptions(engineTypeSelect)), { emptyLabel: 'Любое' })
      setSelectOptions(transmissionSelect, fallbackOptions(data.transmissions, readCurrentSelectOptions(transmissionSelect)), { emptyLabel: 'Любая' })
      setSelectOptions(driveTypeSelect, fallbackOptions(data.drive_types, readCurrentSelectOptions(driveTypeSelect)), { emptyLabel: 'Любой' })
      const regYears = fallbackOptions(data.reg_years, readCurrentSelectOptions(regYearMin))
      if (regYears.length) {
        setSelectOptions(regYearMin, regYears, { emptyLabel: 'Не важно', labelKey: 'value', valueKey: 'value' })
        setSelectOptions(regYearMax, regYears, { emptyLabel: 'Не важно', labelKey: 'value', valueKey: 'value' })
      }
      const colorInput = qs('input[name="color"]', form)
      const basicWrap = qs('.color-swatches--basic[data-color-chips]', form)
      const extraWrap = qs('#colors-extra-search[data-color-chips]', form)
      const toggle = qs('[data-colors-toggle][data-target="colors-extra-search"]', form)
      const basic = Array.isArray(data.colors_basic) ? data.colors_basic : []
      const extra = Array.isArray(data.colors_other) ? data.colors_other : []
      if (basic.length || extra.length) hadLivePayload = true
      if (basicWrap && (basic.length || !basicWrap.children.length)) rebuildColorChipGroup(basicWrap, basic)
      if (extraWrap && (extra.length || !extraWrap.children.length)) rebuildColorChipGroup(extraWrap, extra)
      if (toggle) {
        const renderedExtraCount = extra.length || qsa('.color-chip', extraWrap || form).length
        toggle.classList.toggle('is-hidden', renderedExtraCount === 0)
        if (renderedExtraCount === 0) toggle.setAttribute('aria-expanded', 'false')
      }
      if (colorInput) {
        const allowed = new Set(qsa('.color-chip', form).map((item) => item.dataset.color || '').filter(Boolean))
        const selected = parseSelectedColorValues(colorInput)
        if (selected.length) {
          const nextSelected = selected.filter((value) => allowed.has(value))
          if (nextSelected.length !== selected.length) {
            setSelectedColorValues(colorInput, nextSelected)
          }
        }
      }
      bindColorChips(form, () => {
        scheduleCount()
        scheduleOptionsRefresh()
      })
      bindChoiceChips(form, () => {
        scheduleCount()
        scheduleOptionsRefresh()
      })
      bindMultiSelectMenus(form, () => {
        scheduleCount()
        scheduleOptionsRefresh()
      })
      bindOtherColorsToggle(form)
      syncColorChips(form)
      syncChoiceChips(form)
      syncMultiSelectMenus(form)
      syncAdvancedGenerationVisibility()
      return hadLivePayload
    }

    const applyPayloadOptions = async (data, reqId = '') => {
      if (!data) return
      let payloadHydrated = applyBasicOptions(data)
      const isInitialPayloadMerge = form.dataset.payloadHydrated !== '1'
      await refreshSearchRowsOptions(Array.isArray(data.brands) ? data.brands : [], reqId)
      qsa('[data-region-options]', form).forEach((select) => {
        const name = select.getAttribute('name') || ''
        const base = payloadMap[name]
        if (!base) return
        const eu = Array.isArray(data[`${base}_eu`]) ? data[`${base}_eu`] : []
        const kr = Array.isArray(data[`${base}_kr`]) ? data[`${base}_kr`] : []
        const preserved = isInitialPayloadMerge && !eu.length && !kr.length
        const existingEu = parseOptions(select.dataset.optionsEu)
        const existingKr = parseOptions(select.dataset.optionsKr)
        const nextEu = preserved ? (existingEu.length ? existingEu : readCurrentSelectOptions(select)) : eu
        const nextKr = preserved ? existingKr : kr
        if (eu.length || kr.length) payloadHydrated = true
        select.dataset.optionsEu = JSON.stringify(nextEu)
        select.dataset.optionsKr = JSON.stringify(nextKr)
        const label = select.closest('label')
        if (label) {
          label.dataset.hasEu = nextEu.length ? '1' : '0'
          label.dataset.hasKr = nextKr.length ? '1' : '0'
        }
      })
      qsa('[data-region-chip-options]', form).forEach((wrap) => {
        const base = wrap.dataset.optionBase || ''
        if (!base) return
        const eu = Array.isArray(data[`${base}_eu`]) ? data[`${base}_eu`] : []
        const kr = Array.isArray(data[`${base}_kr`]) ? data[`${base}_kr`] : []
        const preserved = isInitialPayloadMerge && !eu.length && !kr.length
        const existingEu = parseOptions(wrap.dataset.optionsEu)
        const existingKr = parseOptions(wrap.dataset.optionsKr)
        const nextEu = preserved ? (existingEu.length ? existingEu : readRenderedChoiceChipItems(wrap)) : eu
        const nextKr = preserved ? existingKr : kr
        if (eu.length || kr.length) payloadHydrated = true
        wrap.dataset.optionsEu = JSON.stringify(nextEu)
        wrap.dataset.optionsKr = JSON.stringify(nextKr)
        const field = wrap.closest('[data-has-eu]')
        if (field) {
          field.dataset.hasEu = nextEu.length ? '1' : '0'
          field.dataset.hasKr = nextKr.length ? '1' : '0'
        }
      })
      qsa('.advanced-section', form).forEach((section) => {
        let hasEu = false
        let hasKr = false
        qsa('[data-region-options]', section).forEach((select) => {
          const eu = parseOptions(select.dataset.optionsEu)
          const kr = parseOptions(select.dataset.optionsKr)
          if (eu.length) hasEu = true
          if (kr.length) hasKr = true
        })
        qsa('[data-region-chip-options]', section).forEach((wrap) => {
          const eu = parseOptions(wrap.dataset.optionsEu)
          const kr = parseOptions(wrap.dataset.optionsKr)
          if (eu.length) hasEu = true
          if (kr.length) hasKr = true
        })
        section.dataset.hasEu = hasEu ? '1' : '0'
        section.dataset.hasKr = hasKr ? '1' : '0'
      })
      if (payloadHydrated) {
        form.dataset.payloadHydrated = '1'
      }
      updateRegionFilters()
    }

    const loadPayloadOptions = async () => {
      const params = buildParams(false)
      const reqId = String((Number(form.dataset.payloadReqId || '0') || 0) + 1)
      form.dataset.payloadReqId = reqId
      try {
        const res = await fetch(`/api/filter_payload?${params.toString()}`)
        if (!res.ok) return
        const data = await res.json()
        if (form.dataset.payloadReqId !== reqId) return
        await applyPayloadOptions(data, reqId)
      } catch (e) {
        console.warn('filter payload', e)
      }
    }

    const updateRegionFilters = () => {
      const region = regionSelect?.value || ''
      const showFor = (el) => {
        const hasEu = el.dataset.hasEu === '1'
        const hasKr = el.dataset.hasKr === '1'
        if (!region) return hasEu || hasKr
        if (region === 'EU') return hasEu
        if (region === 'KR') return hasKr
        return true
      }
      qsa('[data-has-eu]', form).forEach((el) => {
        if (el.classList && el.classList.contains('advanced-section')) return
        el.classList.toggle('is-hidden', !showFor(el))
      })
      qsa('[data-region-options]', form).forEach((select) => {
        const eu = parseOptions(select.dataset.optionsEu)
        const kr = parseOptions(select.dataset.optionsKr)
        let next = []
        if (!region) {
          next = eu.concat(kr)
        } else if (region === 'KR') {
          next = kr
        } else {
          next = eu
        }
        setRegionSelectOptions(select, next)
      })
      qsa('[data-region-chip-options]', form).forEach((wrap) => {
        const eu = parseOptions(wrap.dataset.optionsEu)
        const kr = parseOptions(wrap.dataset.optionsKr)
        let next = []
        if (!region) {
          next = eu.concat(kr)
        } else if (region === 'KR') {
          next = kr
        } else {
          next = eu
        }
        renderChoiceChips(wrap, next)
        const inputName = wrap.dataset.chipInput || ''
        const input = inputName ? qs(`input[name="${inputName}"]`, form) : null
        if (input) {
          const allowed = new Set(next.map((item) => item.value))
          const selected = parseSelectedCsvValues(input)
          if (selected.length) {
            const filtered = selected.filter((value) => allowed.has(value))
            if (filtered.length !== selected.length) {
              setSelectedCsvValues(input, filtered)
            }
          }
        }
      })
      bindChoiceChips(form, () => {
        scheduleCount()
        scheduleOptionsRefresh()
      })
      bindMultiSelectMenus(form, () => {
        scheduleCount()
        scheduleOptionsRefresh()
      })
      syncChoiceChips(form)
      syncMultiSelectMenus(form)
    }

    const updateRegionSub = () => {
      const region = regionSelect?.value || ''
      const hasKr = Boolean(regionSubKr)
      const showKr = region === 'KR' && hasKr
      const showEu = region === 'EU' || (!region && !showKr)
      if (regionSubEu) {
        regionSubEu.classList.toggle('is-hidden', !showEu)
      }
      if (regionSubKr) {
        regionSubKr.classList.toggle('is-hidden', !showKr)
      }
      if (regionEuSelect) {
        regionEuSelect.disabled = region === 'KR'
      }
      if (regionKrSelect) {
        regionKrSelect.disabled = !showKr
      }
    }

    const syncAdvancedGenerationVisibility = () => {
      if (!advancedGenerationSelect || !advancedGenerationField) return
      const hasItems = Array.from(advancedGenerationSelect.options || []).some((opt) => opt.value)
      advancedGenerationField.classList.toggle('is-hidden', !hasItems)
      advancedGenerationSelect.disabled = !hasItems
      if (!hasItems) advancedGenerationSelect.value = ''
    }

    const shouldRefreshOptionsForControl = (el) => {
      if (!el) return false
      if (el.matches?.('[data-line-model]')) return false
      return true
    }

    const prepareSubmit = () => {
      // rebuild "line" params from rows so backend receives canonical format
      qsa('input[name="line"]', form).forEach((el) => el.remove())
      qsa('[data-line-state-hidden="1"]', form).forEach((el) => el.remove())
      const lines = buildLines()
      lines.forEach((line) => {
        const input = document.createElement('input')
        input.type = 'hidden'
        input.name = 'line'
        input.value = line
        form.appendChild(input)
      })
      const parsedLines = lines.map((line) => parseLineValue(line))
      const uniqueBrands = Array.from(new Set(parsedLines.map((item) => item.brand).filter(Boolean)))
      const uniqueModels = Array.from(new Set(parsedLines.map((item) => item.model).filter(Boolean)))
      const appendStateInput = (name, value) => {
        if (!value) return
        const input = document.createElement('input')
        input.type = 'hidden'
        input.name = name
        input.value = value
        input.dataset.lineStateHidden = '1'
        form.appendChild(input)
      }
      if (uniqueBrands.length === 1) appendStateInput('brand', uniqueBrands[0])
      if (uniqueModels.length === 1) appendStateInput('model', uniqueModels[0])
      // ensure KR submits country=KR even if EU country select is disabled
      const regionVal = String(regionSelect?.value || '').toUpperCase()
      let hiddenCountry = qs('input[type="hidden"][name="country"]', form)
      if (regionVal === 'KR') {
        if (!hiddenCountry) {
          hiddenCountry = document.createElement('input')
          hiddenCountry.type = 'hidden'
          hiddenCountry.name = 'country'
          form.appendChild(hiddenCountry)
        }
        hiddenCountry.value = 'KR'
      } else if (hiddenCountry) {
        hiddenCountry.remove()
      }
    }

    const fillModels = async (brand, modelSelect, selected) => {
      if (!modelSelect) return
      const selectedValues = Array.isArray(selected)
        ? Array.from(new Set(selected.map((value) => String(value || '').trim()).filter(Boolean)))
        : (selected ? [String(selected).trim()] : [])
      if (!brand) {
        fillModelSelectWithGroups(modelSelect, { models: [], model_groups: [] }, 'Неважно')
        modelSelect.disabled = true
        modelSelect.value = ''
        setAccordionSelectedModels(modelSelect, [])
        modelSelect.__modelAccordionSync?.()
        return
      }
      modelSelect.disabled = true
      modelSelect.innerHTML = '<option value="">Загрузка…</option>'
      const region = regionSelect?.value || ''
      const country = regionEuSelect?.value || ''
      const krType = regionKrSelect?.value || ''
      const payload = await fetchModels({ brand, region, country, krType })
      fillModelSelectWithGroups(modelSelect, payload, 'Неважно')
      const resolvedSelectedValues = typeof modelSelect.__resolveModelValues === 'function'
        ? modelSelect.__resolveModelValues(selectedValues)
        : selectedValues
      const availableValues = new Set(
        Array.from(modelSelect.options || [])
          .map((opt) => String(opt.value || '').trim())
          .filter(Boolean),
      )
      const preservedValues = resolvedSelectedValues.filter((value) => availableValues.has(value))
      if (preservedValues.length > 1) {
        modelSelect.value = ''
        setAccordionSelectedModels(modelSelect, preservedValues)
      } else if (preservedValues.length === 1) {
        if (setSelectValueInsensitive(modelSelect, preservedValues[0])) {
          setAccordionSelectedModels(modelSelect, preservedValues)
        } else {
          modelSelect.value = ''
          setAccordionSelectedModels(modelSelect, [])
        }
      } else {
        modelSelect.value = ''
        setAccordionSelectedModels(modelSelect, [])
      }
      modelSelect.disabled = false
      modelSelect.__modelAccordionSync?.()
    }

    const setLineBrandOptions = (select, items) => {
      if (!select) return
      setSelectOptions(select, items, { emptyLabel: 'Неважно', labelKey: 'label', valueKey: 'value' })
      normalizeBrandOptions(select)
    }

    const getRowInitialSelectedModels = (row) => {
      if (!row) return []
      try {
        const parsed = JSON.parse(row.dataset.initialSelectedModels || '[]')
        return Array.isArray(parsed)
          ? parsed.map((value) => String(value || '').trim()).filter(Boolean)
          : []
      } catch {
        return []
      }
    }

    const getRowEffectiveSelectedModels = (row, modelSelect) => {
      const selectedModels = getAccordionSelectedModels(modelSelect)
      if (selectedModels.length) return selectedModels
      const fallbackValue = String(modelSelect?.value || '').trim()
      if (fallbackValue) return [fallbackValue]
      return getRowInitialSelectedModels(row)
    }

    const refreshSearchRowsOptions = async (brandOptions = [], reqId = '') => {
      if (!rowsWrap) return
      form.dataset.lineBrandOptions = JSON.stringify(Array.isArray(brandOptions) ? brandOptions : [])
      const rows = qsa('[data-search-row]', rowsWrap)
      for (const row of rows) {
        if (reqId && form.dataset.payloadReqId !== reqId) return
        const brandSelect = qs('[data-line-brand]', row)
        const modelSelect = qs('[data-line-model]', row)
        if (!brandSelect || !modelSelect) continue
        const currentBrand = normalizeBrand(brandSelect.value || '')
        const currentSelectedModels = getRowEffectiveSelectedModels(row, modelSelect)
        const currentModel = modelSelect.value || ''
        setLineBrandOptions(brandSelect, brandOptions)
        if (currentBrand && !setSelectValueInsensitive(brandSelect, currentBrand)) {
          brandSelect.value = ''
          await fillModels('', modelSelect, '')
          continue
        }
        await fillModels(
          normalizeBrand(brandSelect.value || ''),
          modelSelect,
          currentSelectedModels.length ? currentSelectedModels : currentModel,
        )
      }
    }

    const bindRow = (row, initial = {}) => {
      const brandSelect = qs('[data-line-brand]', row)
      const modelSelect = qs('[data-line-model]', row)
      const removeBtn = qs('[data-line-remove]', row)
      if (brandSelect) {
        const dynamicBrandOptions = parseOptions(form.dataset.lineBrandOptions || '[]')
        if (dynamicBrandOptions.length) {
          setLineBrandOptions(brandSelect, dynamicBrandOptions)
        } else {
          normalizeBrandOptions(brandSelect)
        }
        brandSelect.value = normalizeBrand(initial.brand || '')
      }
      const initialModels = Array.isArray(initial.models)
        ? initial.models
        : (initial.model ? [initial.model] : [])
      row.dataset.initialSelectedModels = JSON.stringify(initialModels)
      Promise.resolve(
        fillModels(normalizeBrand(initial.brand || ''), modelSelect, initial.models || initial.model || '')
      ).then(() => {
        if (initialModels.length) {
          scheduleCount()
        }
      })
      brandSelect?.addEventListener('change', () => {
        fillModels(normalizeBrand(brandSelect.value), modelSelect, '')
        row.dataset.initialSelectedModels = '[]'
        scheduleCount()
        scheduleOptionsRefresh()
      })
      modelSelect?.addEventListener('change', () => {
        row.dataset.initialSelectedModels = '[]'
        scheduleCount()
      })
      removeBtn?.addEventListener('click', () => {
        const rows = qsa('[data-search-row]', rowsWrap)
        if (rows.length <= 1) {
          if (brandSelect) brandSelect.value = ''
          if (modelSelect) modelSelect.value = ''
          fillModels('', modelSelect, '')
          scheduleCount()
          scheduleOptionsRefresh()
          return
        }
        row.remove()
        scheduleCount()
        scheduleOptionsRefresh()
      })
    }

    const bindExistingRow = (row, initial = {}) => {
      if (!row || row.dataset.bound === '1') return
      row.dataset.bound = '1'
      bindRow(row, initial)
    }

    const addRow = (initial = {}) => {
      if (!rowsWrap) return
      let node = null
      if (template?.content) {
        node = template.content.querySelector('[data-search-row]')?.cloneNode(true)
      }
      if (!node) return
      rowsWrap.appendChild(node)
      bindExistingRow(node, initial)
    }

    const forceRebuildRows = (initials = []) => {
      if (!rowsWrap) return
      rowsWrap.replaceChildren()
      const safeInitials = initials.length ? initials : [{}]
      safeInitials.forEach((initial) => addRow(initial))
    }

    const rowsUsable = () => {
      if (!rowsWrap) return false
      const rows = qsa('[data-search-row]', rowsWrap)
      if (!rows.length) return false
      return rows.some((row) => {
        const controls = qsa('select, input', row)
        if (controls.length < 2) return false
        if (row.offsetHeight > 24) return true
        return controls.some((el) => {
          const style = window.getComputedStyle(el)
          return style.display !== 'none' && style.visibility !== 'hidden'
        })
      })
    }

    const ensureRows = (initials = []) => {
      if (!rowsWrap) return
      if (!rowsUsable()) {
        forceRebuildRows(initials)
        return
      }
      const currentRows = qsa('[data-search-row]', rowsWrap)
      if (currentRows.length) {
        currentRows.forEach((row, idx) => bindExistingRow(row, initials[idx] || {}))
        if (currentRows.length < initials.length) {
          initials.slice(currentRows.length).forEach((initial) => addRow(initial))
        }
        return
      }
      forceRebuildRows(initials)
    }

    const buildLines = () => {
      const rows = qsa('[data-search-row]', rowsWrap)
      const lines = []
      rows.forEach((row) => {
        const brand = normalizeBrand(qs('[data-line-brand]', row)?.value || '')
        const modelSelect = qs('[data-line-model]', row)
        const selectedModels = getRowEffectiveSelectedModels(row, modelSelect)
        const models = selectedModels.length ? selectedModels : [modelSelect?.value || '']
        const normalizedModels = Array.from(new Set(models.map((value) => String(value || '').trim())))
        if (!normalizedModels.some(Boolean)) {
          if (!brand) return
          lines.push([brand, '', ''].join('|'))
          return
        }
        normalizedModels.forEach((model) => {
          if (!brand && !model) return
          lines.push([brand, model, ''].map((v) => v.trim()).join('|'))
        })
      })
      return lines
    }

    const buildParams = (withPaging) => {
      const data = new FormData(form)
      const params = new URLSearchParams()
      const lineKeys = ['line_brand', 'line_model']
      for (const [k, v] of data.entries()) {
        if (!v) continue
        if (lineKeys.includes(k)) continue
        params.append(k, v)
      }
      // region-dependent fields
      const regionSel = form.elements['region']
      const countrySel = form.elements['country']
      const krSel = form.elements['kr_type']
      if (regionSel && regionSel.value) {
        params.set('region', String(regionSel.value).toUpperCase())
        if (regionSel.value === 'EU' && countrySel && countrySel.value) {
          params.set('country', String(countrySel.value).toUpperCase())
        }
        if (regionSel.value === 'KR') {
          params.set('country', 'KR')
          if (krSel && krSel.value) params.set('kr_type', krSel.value)
        }
      }
      const lines = buildLines()
      lines.forEach((line) => params.append('line', line))
      const parsedLines = lines.map((line) => parseLineValue(line))
      const uniqueBrands = Array.from(new Set(parsedLines.map((item) => item.brand).filter(Boolean)))
      const uniqueModels = Array.from(new Set(parsedLines.map((item) => item.model).filter(Boolean)))
      if (!params.get('brand') && uniqueBrands.length === 1) {
        params.set('brand', uniqueBrands[0])
      }
      if (!params.get('model') && uniqueModels.length === 1) {
        params.set('model', uniqueModels[0])
      }
      if (withPaging) {
        params.set('page', '1')
        params.set('page_size', '1')
      }
      return params
    }

    let debounce
    let optionsDebounce
    const scheduleCount = () => {
      if (!countEl) return
      clearTimeout(debounce)
      debounce = setTimeout(async () => {
        try {
          const params = buildParams(true)
          const res = await fetch(`/api/advanced_count?${params.toString()}`)
          if (!res.ok) return
          const data = await res.json()
          animateCount(countEl, data.count || 0)
        } catch (e) {
          console.warn('advanced count', e)
        }
      }, 300)
    }

    const scheduleOptionsRefresh = () => {
      clearTimeout(optionsDebounce)
      optionsDebounce = setTimeout(() => {
        loadPayloadOptions()
      }, 250)
    }

    const renderSuggestions = async () => {
      if (!suggestionsEl) return
      suggestionsEl.innerHTML = ''
      const lines = buildLines()
      const first = lines.length ? parseLineValue(lines[0]) : { brand: '', model: '' }
      const params = new URLSearchParams()
      const country = qs('input[name="country"]', form)?.value
      if (country) params.set('country', country)
      if (first.brand) params.set('brand', normalizeBrand(first.brand))
      if (first.model) params.set('model', first.model)
      params.set('page', '1')
      params.set('page_size', '6')
      try {
        const res = await fetch(`/api/cars?${params.toString()}`)
        if (!res.ok) return
        const data = await res.json()
        if (!Array.isArray(data.items) || !data.items.length) return
        const title = document.createElement('div')
        title.className = 'advanced-suggestions__title'
        title.textContent = 'Похожие варианты'
        suggestionsEl.appendChild(title)
        const grid = document.createElement('div')
        grid.className = 'cards'
        data.items.forEach((car) => {
          const card = document.createElement('a')
          card.href = `/car/${car.id}`
          card.className = 'car-card'
          const thumbRaw = car.thumbnail_url || (Array.isArray(car.images) ? car.images[0] : '') || ''
          let thumb = normalizeThumbUrl(thumbRaw, { thumb: true })
          const origThumb = normalizeThumbUrl(thumbRaw)
          const displayRub = car.display_price_rub
          let price = displayRub != null ? formatRub(displayRub) : ''
          if (!price) price = '—'
          const priceNote = car.price_note ? `<div class="price-note">${escapeHtml(car.price_note)}</div>` : ''
          const variantLine = car.variant ? `<div class="car-card__subtitle">${escapeHtml(car.variant)}</div>` : ''
          card.innerHTML = `
            <div class="thumb-wrap">
              <img class="thumb" src="${thumb}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer" data-thumb="${thumb}" data-orig="${origThumb}" data-id="${car.id}" onerror="this.onerror=null;this.src='/static/img/no-photo.svg';" />
            </div>
            <div class="car-card__body">
              <div>
                <div class="car-card__title">${car.brand || ''} ${car.model || ''}</div>
                ${variantLine}
              </div>
              <div class="car-card__price">${price}</div>
              ${priceNote}
            </div>
          `
          const img = card.querySelector('img.thumb')
          if (img) applyThumbFallback(img)
          grid.appendChild(card)
        })
        suggestionsEl.appendChild(grid)
      } catch (e) {
        console.warn('suggestions', e)
      }
    }

    if (form.id !== 'advanced-search-form') return

    form.addEventListener('submit', (event) => {
      event.preventDefault()
      clearTimeout(debounce)
      clearTimeout(optionsDebounce)
      prepareSubmit()
      const params = buildParams(false)
      if (DEBUG_FILTERS) {
        console.info('filters:advanced submit', params.toString())
      }
      window.location.assign(buildCatalogUrl(params))
    })

    form.addEventListener('reset', () => {
      setTimeout(() => {
        rowsWrap?.replaceChildren()
        ensureRows([{}])
        bindRegionSelect(form)
        bindRegMonthState(form)
        bindOtherColorsToggle(form)
        syncColorChips(form)
        updateRegionFilters()
        scheduleCount()
        scheduleOptionsRefresh()
      }, 0)
    })

    addBtn?.addEventListener('click', () => {
      addRow({})
      scheduleCount()
      scheduleOptionsRefresh()
    })

    const initialParams = new URLSearchParams(window.location.search)
    const initialLines = groupLineSelections(initialParams.getAll('line'))
    if (!initialLines.length) {
      const brand = normalizeBrand(initialParams.get('brand') || '')
      const model = (initialParams.get('model') || '').trim()
      if (brand || model) {
        initialLines.push({ brand, models: model ? [model] : [], variant: '' })
      }
    }
    const repairRows = () => ensureRows(initialLines)
    repairRows()
    requestAnimationFrame(repairRows)
    window.addEventListener('load', repairRows)
    window.addEventListener('pageshow', repairRows)
    window.setTimeout(repairRows, 250)
    window.setTimeout(repairRows, 1000)
    if (window.MutationObserver && rowsWrap) {
      const observer = new MutationObserver(() => {
        if (!rowsUsable()) {
          repairRows()
        }
      })
      observer.observe(rowsWrap, { childList: true, subtree: true, attributes: true, attributeFilter: ['class', 'style', 'hidden'] })
    }
    if (form.id !== 'advanced-search-form') {
      bindRegionSelect(form)
    }
    updateRegionSub()
    updateRegionFilters()
    loadPayloadOptions()
    syncAdvancedGenerationVisibility()
    bindRegMonthState(form)
    bindColorChips(form, () => {
      scheduleCount()
      scheduleOptionsRefresh()
    })
    bindChoiceChips(form, () => {
      scheduleCount()
      scheduleOptionsRefresh()
    })
    bindOtherColorsToggle(form)
    syncChoiceChips(form)
    const ctrls = qsa('input, select', form)
    ctrls.forEach((el) => {
      el.addEventListener('change', () => {
        scheduleCount()
        if (shouldRefreshOptionsForControl(el)) {
          scheduleOptionsRefresh()
        }
      })
      el.addEventListener('input', scheduleCount)
    })
    regionSelect?.addEventListener('change', () => {
      updateRegionSub()
      scheduleOptionsRefresh()
      updateRegionFilters()
      syncAdvancedGenerationVisibility()
      scheduleCount()
    })
    scheduleCount()
  }

  async function convertInlinePrices() {
    const fx = await getFx()
    document.querySelectorAll('.js-price').forEach((el) => {
      const price = parseFloat(el.dataset.price)
      const cur = el.dataset.currency || ''
      if (Number.isFinite(price)) {
        const rub = priceToRub(price, cur, fx)
        if (rub != null) {
          el.textContent = formatRub(rub)
        }
      }
    })
  }

  function initBackToTop() {
    const btn = qs('#backToTop')
    if (!btn) return
    const prefersReduce = window.matchMedia('(prefers-reduced-motion: reduce)')
    const toggle = () => {
      const show = window.scrollY > 600
      btn.classList.toggle('is-visible', show)
    }
    window.addEventListener('scroll', toggle, { passive: true })
    toggle()
    btn.addEventListener('click', (e) => {
      e.preventDefault()
      window.scrollTo({
        top: 0,
        behavior: prefersReduce.matches ? 'auto' : 'smooth',
      })
    })
  }

  function initExpandToggles() {
    qsa('[data-expand-toggle]').forEach((btn) => {
      if (btn.dataset.bound === '1') return
      btn.dataset.bound = '1'
      const targetId = btn.getAttribute('data-expand-toggle')
      const target = targetId ? document.getElementById(targetId) : null
      if (!target) return
      const pagedItems = qsa('[data-expand-page]', target)
      const maxPages = Number(btn.dataset.expandPages || 0)
      const expandLabel = btn.dataset.expandLabel || 'Показать ещё предложения'
      const catalogTargetId = btn.dataset.expandCatalogId || ''
      const catalogTarget = catalogTargetId ? document.getElementById(catalogTargetId) : null
      btn.addEventListener('click', (event) => {
        event.preventDefault()
        if (maxPages > 0 && pagedItems.length) {
          const currentPage = Number(btn.dataset.expandCurrentPage || 0)
          const nextPage = Math.min(currentPage + 1, maxPages)
          target.hidden = false
          target.classList.remove('is-collapsed')
          pagedItems.forEach((item) => {
            const pageNo = Number(item.getAttribute('data-expand-page') || 0)
            if (pageNo > 0 && pageNo <= nextPage) {
              item.hidden = false
            }
          })
          btn.dataset.expandCurrentPage = String(nextPage)
          btn.setAttribute('aria-expanded', nextPage >= 1 ? 'true' : 'false')
          if (nextPage >= maxPages) {
            btn.hidden = true
            if (catalogTarget) catalogTarget.hidden = false
          } else {
            btn.textContent = expandLabel
          }
          return
        }
        const expanded = btn.getAttribute('aria-expanded') === 'true'
        const nextExpanded = !expanded
        btn.setAttribute('aria-expanded', nextExpanded ? 'true' : 'false')
        target.hidden = !nextExpanded
        target.classList.toggle('is-collapsed', !nextExpanded)
        btn.textContent = nextExpanded ? 'Скрыть предложения' : 'Все предложения'
      })
    })
  }

  function initDetailGallery() {
    const main = qs('.detail-hero__main')
    const img = qs('#primaryImage')
    if (!main || !img) return
    if (main.dataset.bound === '1') return
    main.dataset.bound = '1'
    window.__detailGalleryHandled = true
    let images = []
    try {
      images = JSON.parse(main.dataset.images || '[]')
    } catch (e) {
      images = []
    }
    if (!Array.isArray(images) || images.length < 2) return
    const deduped = []
    const seen = new Set()
    images.forEach((u) => {
      const normalized = normalizeThumbUrl(u, { thumb: false })
      if (!normalized || seen.has(normalized)) return
      seen.add(normalized)
      deduped.push(normalized)
    })
    images = deduped
    if (images.length < 2) return
    const isUsable = (src) => Boolean(src && src !== '/static/img/no-photo.svg')
    const currentOrig = normalizeThumbUrl(img.getAttribute('data-orig') || img.getAttribute('src') || '', { thumb: false })
    let idx = Math.max(0, images.indexOf(currentOrig))
    if (!isUsable(images[idx])) {
      const firstOk = images.findIndex((src) => isUsable(src))
      idx = firstOk >= 0 ? firstOk : 0
    }
    let thumbs = qsa('.detail-hero__thumbs .thumb')
    if (thumbs.length > images.length) {
      thumbs.slice(images.length).forEach((btn) => btn.remove())
      thumbs = qsa('.detail-hero__thumbs .thumb')
    }
    const thumbsWrap = qs('.detail-hero__thumbs')
    const prevBtn = qs('[data-detail-prev]')
    const nextBtn = qs('[data-detail-next]')
    const photoCount = qs('#detailPhotoCount')
    const markThumbState = (i) => {
      const btn = thumbs[i]
      if (!btn) return
      const broken = !isUsable(images[i])
      btn.classList.toggle('is-broken', broken)
      btn.disabled = broken
      btn.hidden = broken
    }
    const refreshGalleryChrome = () => {
      const usableCount = images.filter((src) => isUsable(src)).length
      if (photoCount) photoCount.textContent = String(usableCount)
      if (thumbsWrap) {
        thumbsWrap.hidden = usableCount <= 1
      }
      if (prevBtn) prevBtn.hidden = usableCount <= 1
      if (nextBtn) nextBtn.hidden = usableCount <= 1
    }
    const syncActive = () => {
      thumbs.forEach((btn, i) => btn.classList.toggle('active', i === idx))
    }
    const setImageByIndex = (nextIdx) => {
      if (!images.length) return
      const normalizedIdx = (nextIdx + images.length) % images.length
      if (!isUsable(images[normalizedIdx])) return
      idx = normalizedIdx
      const nextOrig = images[idx]
      const nextThumb = normalizeThumbUrl(nextOrig, { thumb: true, width: DETAIL_PRIMARY_WIDTH })
      img.dataset.orig = nextOrig
      img.dataset.thumb = nextThumb
      delete img.dataset.thumbRetried
      delete img.dataset.thumbFallbackTried
      delete img.dataset.fallbackApplied
      img.src = nextThumb && nextThumb !== '/static/img/no-photo.svg' ? nextThumb : nextOrig
      applyThumbFallback(img)
      syncActive()
    }
    const move = (step) => {
      if (!images.length) return
      for (let i = 1; i <= images.length; i += 1) {
        const candidate = (idx + step * i + images.length) % images.length
        if (isUsable(images[candidate])) {
          setImageByIndex(candidate)
          return
        }
      }
      img.src = '/static/img/no-photo.svg'
    }
    thumbs.forEach((btn, i) => {
      const thumbImg = btn.querySelector('img')
      markThumbState(i)
      thumbImg?.addEventListener('error', () => {
        images[i] = '/static/img/no-photo.svg'
        markThumbState(i)
        refreshGalleryChrome()
        if (i === idx) move(1)
      })
    })
    setImageByIndex(idx)
    applyThumbFallback(img)
    refreshGalleryChrome()
    prevBtn?.addEventListener('click', (e) => {
      e.preventDefault()
      move(-1)
    })
    nextBtn?.addEventListener('click', (e) => {
      e.preventDefault()
      move(1)
    })
    thumbs.forEach((btn, i) => {
      btn.addEventListener('click', () => {
        if (!isUsable(images[i])) return
        setImageByIndex(i)
      })
    })
    img.addEventListener('error', () => {
      images[idx] = '/static/img/no-photo.svg'
      markThumbState(idx)
      refreshGalleryChrome()
      move(1)
    })
    let startX = null
    main.addEventListener('touchstart', (e) => {
      if (e.touches.length) startX = e.touches[0].clientX
    })
    main.addEventListener('touchend', (e) => {
      if (startX == null) return
      const endX = e.changedTouches[0].clientX
      const diff = endX - startX
      if (Math.abs(diff) > 30) {
        move(diff > 0 ? -1 : 1)
      }
      startX = null
    })
    syncActive()
    refreshGalleryChrome()
    if (isUsable(images[idx])) {
      setImageByIndex(idx)
    } else {
      move(1)
    }
  }

  function initDetailActions() {
    const calcBtn = qs('#toggleCalc')
    const calcBlock = qs('#calcBreakdown')
    if (calcBtn && calcBlock) {
      calcBtn.addEventListener('click', (e) => {
        e.preventDefault()
        const hidden = getComputedStyle(calcBlock).display === 'none'
        calcBlock.style.display = hidden ? '' : 'none'
        calcBtn.setAttribute('aria-expanded', hidden ? 'true' : 'false')
      })
    }
    const leadBtn = qs('#detail-lead-btn')
    const leadForm = qs('#detail-lead-form')
    if (leadBtn && leadForm) {
      leadBtn.addEventListener('click', (e) => {
        e.preventDefault()
        const hidden = getComputedStyle(leadForm).display === 'none'
        leadForm.style.display = hidden ? '' : 'none'
        leadBtn.setAttribute('aria-expanded', hidden ? 'true' : 'false')
        if (hidden) {
          leadForm.scrollIntoView({ behavior: 'smooth', block: 'start' })
        }
      })
    }
  }

  function initThumbFallbacks() {
    qsa('img.thumb').forEach((img) => applyThumbFallback(img))
    const primary = qs('#primaryImage')
    if (primary) applyThumbFallback(primary)
  }

  function initAll() {
    const safeInit = (label, fn) => {
      try {
        const result = fn()
        if (result && typeof result.then === 'function') {
          result.catch((e) => {
            console.error(`[la:init:${label}]`, e)
          })
        }
      } catch (e) {
        console.error(`[la:init:${label}]`, e)
      }
    }
    safeInit('nav', initNav)
    safeInit('favorites', loadFavoritesState)
    safeInit('catalog', initCatalog)
    safeInit('home', initHome)
    safeInit('expand-toggles', initExpandToggles)
    safeInit('advanced-search', initAdvancedSearch)
    safeInit('detail-gallery', initDetailGallery)
    safeInit('detail-actions', initDetailActions)
    safeInit('lead-prefill', applyLeadPrefill)
    safeInit('lead-from-detail', initLeadFromDetail)
    safeInit('back-to-top', initBackToTop)
    safeInit('thumb-fallbacks', initThumbFallbacks)
    safeInit('inline-prices', convertInlinePrices)
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initAll)
  } else {
    initAll()
  }
  window.LA_APP_INIT = initAll
})()
