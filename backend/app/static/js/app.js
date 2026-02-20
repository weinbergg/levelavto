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
    const rounded = Math.ceil(n / 10000) * 10000
    return `${rounded.toLocaleString('ru-RU')} ₽`
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

  const THUMB_REV = '2'

  function thumbProxyUrl(url, width = 360) {
    if (!url) return '/static/img/no-photo.svg'
    if (url.startsWith('/thumb?')) {
      return url.includes('rev=') ? url : `${url}&rev=${THUMB_REV}`
    }
    if (!url.includes('img.classistatic.de')) return url
    return `/thumb?u=${encodeURIComponent(url)}&w=${width}&fmt=webp&rev=${THUMB_REV}`
  }

  function normalizeThumbUrl(src, opts = {}) {
    const val = String(src || '').trim()
    if (!val) return '/static/img/no-photo.svg'
    let url = val
    while (url.startsWith('.')) url = url.slice(1)
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
      return thumbProxyUrl(url)
    }
    return url
  }

  function applyThumbFallback(img) {
    if (!img) return
    const rawSrc = img.getAttribute('src') || ''
    const rawData = img.dataset.thumb || ''
    const rawOrig = img.dataset.orig || ''
    let normalized = normalizeThumbUrl(rawSrc, { thumb: true })
    if (normalized === '/static/img/no-photo.svg' && rawData) {
      const dataNormalized = normalizeThumbUrl(rawData, { thumb: true })
      if (dataNormalized !== '/static/img/no-photo.svg') {
        normalized = dataNormalized
      }
    }
    if (normalized === '/static/img/no-photo.svg' && rawOrig) {
      const origNormalized = normalizeThumbUrl(rawOrig)
      if (origNormalized !== '/static/img/no-photo.svg') {
        normalized = origNormalized
      }
    }
    if (normalized === '/static/img/no-photo.svg' && rawSrc) {
      const trimmed = String(rawSrc || '').trim()
      if (trimmed.startsWith('http://') || trimmed.startsWith('https://') || trimmed.startsWith('/')) {
        normalized = trimmed
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
        // First failure on proxy thumb can be transient (e.g. lock busy -> 503).
        // Retry once with cache-busting before falling back to origin/static placeholder.
        const currentSrc = img.getAttribute('src') || ''
        if (!img.dataset.thumbRetried && currentSrc.startsWith('/thumb?')) {
          img.dataset.thumbRetried = '1'
          const retrySrc = `${currentSrc}${currentSrc.includes('?') ? '&' : '?'}rt=${Date.now()}`
          setTimeout(() => {
            img.src = retrySrc
          }, 120)
          return
        }
        if (img.dataset.fallbackApplied === '1') return
        img.dataset.fallbackApplied = '1'
        const orig = img.dataset.orig || ''
        const origNormalized = normalizeThumbUrl(orig)
        if (origNormalized && origNormalized !== '/static/img/no-photo.svg') {
          img.src = origNormalized
          img.dataset.fallbackApplied = '2'
          return
        }
        img.src = '/static/img/no-photo.svg'
      }
    }
  }

  function setSelectOptions(select, items, { emptyLabel = 'Все', valueKey = 'value', labelKey = 'label' } = {}) {
    if (!select) return
    const current = select.value
    const normalizedItems = (items || []).filter((item) => {
      const isObj = item && typeof item === 'object'
      const value = isObj ? (item[valueKey] ?? item.value) : item
      return value != null && value !== ''
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
    ;(colors || []).forEach((c) => {
      const btn = document.createElement('button')
      btn.type = 'button'
      btn.className = 'color-chip'
      btn.dataset.color = c.value
      btn.dataset.label = c.label
      btn.title = c.label
      btn.setAttribute('aria-label', c.label)
      if (c.hex) btn.style.setProperty('--chip-color', c.hex)
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
    return selected
  }

  function syncFormFromSelected(form, selected) {
    if (!form || !selected) return
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

  function syncColorChips(scope) {
    if (!scope) return
    const input = qs('input[name="color"]', scope)
    const chips = qsa('.color-chip', scope)
    if (!input || !chips.length) return
    const value = input.value
    chips.forEach((chip) => {
      chip.classList.toggle('active', chip.dataset.color === value)
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
        if (input.value === next) {
          input.value = ''
        } else {
          input.value = next
        }
        syncColorChips(scope)
        if (typeof onChange === 'function') onChange()
      })
    })
    syncColorChips(scope)
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
      const input = qs('input[name="color"]', scope)
      const update = (expanded) => {
        toggle.setAttribute('aria-expanded', expanded ? 'true' : 'false')
        extra.classList.toggle('is-collapsed', !expanded)
        toggle.textContent = expanded ? 'Скрыть цвета' : 'Другие цвета'
      }
      toggle.addEventListener('click', () => {
        const expanded = toggle.getAttribute('aria-expanded') === 'true'
        update(!expanded)
      })
      toggle.dataset.bound = '1'
      if (input?.value) {
        const hasInExtra = extra.querySelector(`.color-chip[data-color="${input.value}"]`)
        if (hasInExtra) {
          update(true)
          return
        }
      }
      update(false)
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
      label.classList.toggle('is-hidden', !hasYear)
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
    countrySelect?.addEventListener('change', update)
    krSelect?.addEventListener('change', update)
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
      interior_design: 'Интерьер',
      price_rating_label: 'Оценка цены',
      color: 'Цвет',
      q: 'Поиск',
      region: null,
      kr_type: null,
      interior_color: null,
      interior_material: null,
      air_suspension: 'Пневмоподвеска',
      reg_year_min: null,
      reg_month_min: null,
      reg_year_max: null,
      reg_month_max: null,
      line: null,
      sort: null,
    }
    const selectLabel = (name, value) => {
      const el = form.elements[name]
      if (!el || !value || el.tagName !== 'SELECT') return value
      const opt = Array.from(el.options || []).find((o) => o.value === value)
      return opt ? opt.textContent.trim() : value
    }
    const countryLabel = (value) => {
      const map = window.COUNTRY_LABELS || {}
      return map[value] || value
    }
    const colorLabel = (value) => {
      const chip = form.querySelector(`.color-chip[data-color="${value}"]`)
      return chip?.dataset?.label || value
    }
    const regMinYear = params.get('reg_year_min')
    const regMinMonth = params.get('reg_month_min')
    const regMaxYear = params.get('reg_year_max')
    const regMaxMonth = params.get('reg_month_max')
    const chips = []
    params.forEach((value, key) => {
      if (!value || ['page', 'page_size'].includes(key)) return
      // skip sort in active chips to avoid debug-look
      if (key === 'sort') return
      const label = labels[key] || key
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
      if (['engine_type', 'transmission', 'drive_type', 'body_type', 'condition'].includes(key)) {
        displayValue = selectLabel(key, value)
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
        displayValue = colorLabel(value)
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
    chips.forEach(({ key, keys, label, value }) => {
      const chip = document.createElement('button')
      chip.type = 'button'
      chip.className = 'filter-chip'
      chip.innerHTML = `<span>${label}: ${value}</span><span class="chip-close">×</span>`
      chip.addEventListener('click', () => {
        const toClear = keys || [key]
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
        }
        loadCars(1)
      })
      container.appendChild(chip)
    })
  }

  function collectParams(page) {
    const form = qs('#filters')
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
      syncColorChips(form)
      syncRegMonthState(form)
      bindOtherColorsToggle(form)
      const modelSelect = qs('#model-select')
      if (modelSelect) {
        modelSelect.innerHTML = '<option value=\"\">Все</option>'
        modelSelect.disabled = true
      }
      loadCars(1)
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

  async function loadCars(page = 1) {
    const spinner = qs('#spinner')
    const cards = qs('#cards')
    const pageInfo = qs('#pageInfo')
    const resultCount = qs('#resultCount')
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
    try {
      const fx = await getFx()
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
      if (pageInfo) {
      pageInfo.textContent = `Страница ${data.page} из ${Math.max(1, Math.ceil(data.total / data.page_size))}`
      }
      const pageNumbers = qs('#pageNumbers')
      const totalPages = Math.max(1, Math.ceil(data.total / data.page_size))
      if (pageNumbers) {
        pageNumbers.innerHTML = ''
        const addBtn = (p, label = null) => {
          const b = document.createElement('button')
          b.className = 'btn page-btn' + (p === data.page ? ' active' : '')
          b.textContent = label || String(p)
          b.addEventListener('click', () => loadCars(p))
          pageNumbers.appendChild(b)
        }
        addBtn(1)
        const windowSize = 5
        const start = Math.max(2, data.page - 2)
        const end = Math.min(totalPages - 1, start + windowSize - 1)
        if (start > 2) {
          const dots = document.createElement('span')
          dots.className = 'page-dots'
          dots.textContent = '…'
          pageNumbers.appendChild(dots)
        }
        for (let p = start; p <= end; p++) addBtn(p)
        if (end < totalPages - 1) {
          const dots = document.createElement('span')
          dots.className = 'page-dots'
          dots.textContent = '…'
          pageNumbers.appendChild(dots)
        }
        if (totalPages > 1) addBtn(totalPages)
      }

      if (resultCount) {
        if (data.total === 0) {
          resultCount.textContent = 'Ничего не найдено. Измените фильтры.'
        } else {
          const from = (data.page - 1) * data.page_size + 1
          const to = Math.min(data.total, data.page * data.page_size)
          resultCount.textContent = `Показано ${from}-${to} из ${data.total}`
        }
      }

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
            if (src !== '/static/img/no-photo.svg') {
              img.dataset.thumb = src
              img.setAttribute('src', src)
            }
            if (orig && orig !== '/static/img/no-photo.svg') {
              img.dataset.orig = orig
            }
            applyThumbFallback(img)
          }
        })
        bindFavoriteButtons(cards)
        window.__page = data.page
        window.__pageSize = data.page_size
        window.__total = data.total
        cards.dataset.ssr = '0'
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
        window.scrollTo({ top: Number(saved) || 0, behavior: 'instant' })
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
    const initialSort = urlParams.get('sort') || 'price_asc'
    const modelSelect = qs('#model-select')
    const brandSelect = qs('#brand')
    const generationSelect = qs('#generation')
    const generationField = generationSelect ? generationSelect.closest('[data-generation-field]') : null
    const advancedLink = qs('#catalog-advanced-link')
    normalizeBrandOptions(brandSelect)
    let initialReapplyDone = false
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
        setSelectOptions(qs('#body_type'), data.body_types || [], { emptyLabel: 'Любой' })
        setSelectOptions(qs('[name="engine_type"]'), data.engine_types || [], { emptyLabel: 'Любое' })
        setSelectOptions(qs('[name="transmission"]'), data.transmissions || [], { emptyLabel: 'Любая' })
        setSelectOptions(qs('[name="drive_type"]'), data.drive_types || [], { emptyLabel: 'Любой' })
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
        renderColorChips(basic, data.colors_basic || [])
        renderColorChips(extra, data.colors_other || [])
        if (DEBUG_FILTERS) {
          console.info('catalog: ctx loaded countries=' + (data.countries || []).length + ' kr_types=' + (data.kr_types || []).length)
        }
        bindColorChips(filtersForm, () => loadCars(1))
        bindOtherColorsToggle(filtersForm)
        syncColorChips(filtersForm)
      } catch (e) {
        console.warn('filters base', e)
      }
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
      syncColorChips(form)
      syncRegMonthState(form)
      bindOtherColorsToggle(form)
      bindRegionSelect(form)
      if (modelSelect) {
        modelSelect.innerHTML = '<option value="">Все</option>'
        modelSelect.disabled = true
      }
      sessionStorage.setItem('catalogScroll', String(0))
      loadCars(1)
    })

    qs('#prevPage')?.addEventListener('click', () => {
      const p = Math.max(1, (window.__page || 1) - 1)
      loadCars(p)
    })
    qs('#nextPage')?.addEventListener('click', () => {
      const max = Math.max(1, Math.ceil((window.__total || 0) / (window.__pageSize || 12)))
      const p = Math.min(max, (window.__page || 1) + 1)
      loadCars(p)
    })

    if (filtersForm) {
      bindColorChips(filtersForm, () => loadCars(1))
      bindOtherColorsToggle(filtersForm)
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
      const trigger = () => {
        clearTimeout(debounce)
        debounce = setTimeout(() => {
          const params = collectParams(1)
          updateCatalogUrlFromParams(params)
          if (DEBUG_FILTERS) console.info('catalog: loadCars params', params.toString())
          loadCars(1)
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
      qs('#region')?.addEventListener('change', loadCatalogFilterBase)
      qs('#country')?.addEventListener('change', loadCatalogFilterBase)
      const sortSelect = qs('#sortHidden', filtersForm)
      if (sortSelect && initialSort) sortSelect.value = initialSort
      const sortTopbar = qs('#sort-select')
      if (sortTopbar && initialSort) sortTopbar.value = initialSort
      if (sortTopbar) {
        sortTopbar.addEventListener('change', () => {
          const val = sortTopbar.value
          if (sortSelect) sortSelect.value = val
          loadCars(1)
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
      modelSelect.innerHTML = ''
      if (!normBrand) {
        modelSelect.disabled = true
        modelSelect.innerHTML = '<option value="">Все</option>'
        return
      }
      modelSelect.disabled = true
      modelSelect.innerHTML = '<option value="">Загрузка…</option>'
      const region = qs('[name="region"]')?.value || ''
      const country = qs('[name="country"]')?.value || ''
      const krType = qs('[name="kr_type"]')?.value || ''
      const payload = await fetchModels({ brand: normBrand, region, country, krType })
      fillModelSelectWithGroups(modelSelect, payload, 'Все')
      if (initialModelParam) {
        setSelectValueInsensitive(modelSelect, initialModelParam)
      }
      modelSelect.disabled = false
    }
    brandSelect?.addEventListener('change', () => {
      updateCatalogModels().then(() => loadCars(1))
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
      await loadCatalogFilterBase()
      if (!initialReapplyDone) {
        reapplySelected()
        initialReapplyDone = true
      }
      if (brandSelect && brandSelect.value) {
        await updateCatalogModels()
        if (initialModelParam) setSelectValueInsensitive(modelSelect, initialModelParam)
      }
      syncGenerationVisibility()
      loadCars(initialPage)
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

    const removeAccordion = () => {
      const host = select.closest('.field, label, .search-row')
      const key = select.id || select.name || 'model'
      const container = host?.querySelector?.(`[data-model-accordion-for="${key}"]`)
      if (container) container.remove()
      select.classList.remove('model-select-native')
    }

    const setAccordionState = (container, selectedValue) => {
      if (!container) return
      qsa('[data-model-value]', container).forEach((btn) => {
        const active = String(btn.dataset.modelValue || '') === String(selectedValue || '')
        btn.classList.toggle('is-active', active)
      })
      const selectedEl = qs('[data-model-selected]', container)
      if (selectedEl) {
        const selectedOpt = Array.from(select.options || []).find((o) => String(o.value) === String(selectedValue || ''))
        selectedEl.textContent = selectedOpt?.textContent || emptyLabel
      }
    }

    const renderAccordion = () => {
      if (!enableAccordion) {
        removeAccordion()
        return
      }
      const host = select.closest('.field, label, .search-row')
      if (!host) return
      let container = host.querySelector(`[data-model-accordion-for="${select.id || select.name || 'model'}"]`)
      if (!container) {
        container = document.createElement('div')
        container.className = 'model-accordion'
        container.dataset.modelAccordionFor = select.id || select.name || 'model'
        host.appendChild(container)
      }
      if (!groups.length) {
        container.innerHTML = ''
        container.classList.add('is-hidden')
        select.classList.remove('model-select-native')
        return
      }

      select.classList.add('model-select-native')
      container.classList.remove('is-hidden')
      container.innerHTML = ''

      const selected = document.createElement('div')
      selected.className = 'model-accordion__selected'
      selected.innerHTML = `
        <span class="muted">Выбрано:</span>
        <strong data-model-selected>${emptyLabel}</strong>
      `
      container.appendChild(selected)

      const root = document.createElement('details')
      root.className = 'model-accordion__root'
      const rootSummary = document.createElement('summary')
      rootSummary.textContent = 'Серии и модели'
      root.appendChild(rootSummary)
      const rootBody = document.createElement('div')
      rootBody.className = 'model-accordion__body'
      root.appendChild(rootBody)
      container.appendChild(root)

      const clearBtn = document.createElement('button')
      clearBtn.type = 'button'
      clearBtn.className = 'btn btn-ghost btn-small'
      clearBtn.textContent = emptyLabel
      clearBtn.addEventListener('click', () => {
        select.value = ''
        select.dispatchEvent(new Event('change', { bubbles: true }))
        setAccordionState(container, '')
        root.open = false
      })
      rootBody.appendChild(clearBtn)

      groups.forEach((group) => {
        const details = document.createElement('details')
        details.className = 'model-accordion__group'
        const summary = document.createElement('summary')
        const count = Number(group?.count || 0)
        summary.textContent = `${group?.label || 'Прочее'}${count ? ` (${count})` : ''}`
        details.appendChild(summary)

        const modelsWrap = document.createElement('div')
        modelsWrap.className = 'model-accordion__models'
        const groupModels = Array.isArray(group?.models) ? group.models : []
        groupModels.forEach((row) => {
          const value = row?.value || row?.model || ''
          if (!value) return
          const btn = document.createElement('button')
          btn.type = 'button'
          btn.className = 'model-accordion__model'
          btn.dataset.modelValue = value
          btn.textContent = row?.label || value
          btn.addEventListener('click', () => {
            select.value = value
            select.dispatchEvent(new Event('change', { bubbles: true }))
            setAccordionState(container, value)
            root.open = false
          })
          modelsWrap.appendChild(btn)
        })
        details.appendChild(modelsWrap)
        rootBody.appendChild(details)
      })

      setAccordionState(container, select.value || '')
      if (!select.dataset.modelAccordionBound) {
        select.addEventListener('change', () => {
          setAccordionState(container, select.value || '')
        })
        select.dataset.modelAccordionBound = '1'
      }
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

    if (resetBtn) {
      resetBtn.addEventListener('click', (e) => {
        e.preventDefault()
        form.reset()
        bindRegionSelect(form)
        if (modelSelect) {
          modelSelect.innerHTML = '<option value="">Все</option>'
          modelSelect.disabled = true
        }
        initialAnimation = true
        updateCount()
      })
    }

    async function updateHomeModels() {
      if (!brandSelect || !modelSelect) return
      const brand = normalizeBrand(brandSelect.value)
      modelSelect.innerHTML = ''
      if (!brand) {
        modelSelect.disabled = true
        modelSelect.innerHTML = '<option value="">Все</option>'
        return
      }
      modelSelect.disabled = true
      modelSelect.innerHTML = '<option value="">Загрузка…</option>'
      const region = regionSelect?.value || ''
      const country = regionSlotSelect?.name === 'country' ? (regionSlotSelect?.value || '') : ''
      const krType = regionSlotSelect?.name === 'kr_type' ? (regionSlotSelect?.value || '') : ''
      const payload = await fetchModels({ brand, region, country, krType })
      fillModelSelectWithGroups(modelSelect, payload, 'Все')
      modelSelect.disabled = false
    }

    brandSelect?.addEventListener('change', () => {
      updateHomeModels().then(() => updateCount())
    })

    function buildHomeParams(withPaging = false) {
      const data = new FormData(form)
      const params = new URLSearchParams()
      const numericKeys = ['price_max', 'mileage_max', 'reg_year_min', 'reg_year_max']
      const skipKeys = ['region_extra']
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
      updateCount()
    })
    regionSlotSelect?.addEventListener('change', updateCount)
    // Do not intercept home form submit; use plain GET /catalog with form fields.
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
      interior_design: 'interior_design_options',
      price_rating_label: 'price_rating_labels',
    }

    const applyPayloadOptions = (data) => {
      if (!data) return
      qsa('[data-region-options]', form).forEach((select) => {
        const name = select.getAttribute('name') || ''
        const base = payloadMap[name]
        if (!base) return
        const eu = Array.isArray(data[`${base}_eu`]) ? data[`${base}_eu`] : []
        const kr = Array.isArray(data[`${base}_kr`]) ? data[`${base}_kr`] : []
        select.dataset.optionsEu = JSON.stringify(eu)
        select.dataset.optionsKr = JSON.stringify(kr)
        const label = select.closest('label')
        if (label) {
          label.dataset.hasEu = eu.length ? '1' : '0'
          label.dataset.hasKr = kr.length ? '1' : '0'
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
        section.dataset.hasEu = hasEu ? '1' : '0'
        section.dataset.hasKr = hasKr ? '1' : '0'
      })
      updateRegionFilters()
    }

    const loadPayloadOptions = async () => {
      const params = new URLSearchParams()
      const region = regionSelect?.value || ''
      const country = regionEuSelect?.value || ''
      if (region) params.set('region', region)
      if (country) params.set('country', country)
      try {
        const res = await fetch(`/api/filter_payload?${params.toString()}`)
        if (!res.ok) return
        const data = await res.json()
        applyPayloadOptions(data)
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

    const parseLine = (line) => {
      const parts = String(line || '').split('|')
      return {
        brand: normalizeBrand(parts[0] || ''),
        model: (parts[1] || '').trim(),
        variant: (parts[2] || '').trim(),
      }
    }

    const prepareSubmit = () => {
      // rebuild "line" params from rows so backend receives canonical format
      qsa('input[name="line"]', form).forEach((el) => el.remove())
      const lines = buildLines()
      lines.forEach((line) => {
        const input = document.createElement('input')
        input.type = 'hidden'
        input.name = 'line'
        input.value = line
        form.appendChild(input)
      })
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
      if (!brand) {
        modelSelect.disabled = true
        modelSelect.innerHTML = '<option value="">Неважно</option>'
        return
      }
      modelSelect.disabled = true
      modelSelect.innerHTML = '<option value="">Загрузка…</option>'
      const region = regionSelect?.value || ''
      const country = regionEuSelect?.value || ''
      const krType = regionKrSelect?.value || ''
      const payload = await fetchModels({ brand, region, country, krType })
      fillModelSelectWithGroups(modelSelect, payload, 'Неважно')
      if (selected) modelSelect.value = selected
      modelSelect.disabled = false
    }

    const bindRow = (row, initial = {}) => {
      const brandSelect = qs('[data-line-brand]', row)
      const modelSelect = qs('[data-line-model]', row)
      const variantInput = qs('[data-line-variant]', row)
      const removeBtn = qs('[data-line-remove]', row)
      if (brandSelect) {
        normalizeBrandOptions(brandSelect)
        brandSelect.value = normalizeBrand(initial.brand || '')
      }
      if (variantInput) variantInput.value = initial.variant || ''
      fillModels(normalizeBrand(initial.brand || ''), modelSelect, initial.model || '')
      brandSelect?.addEventListener('change', () => {
        fillModels(normalizeBrand(brandSelect.value), modelSelect, '')
        scheduleCount()
      })
      modelSelect?.addEventListener('change', scheduleCount)
      variantInput?.addEventListener('input', scheduleCount)
      removeBtn?.addEventListener('click', () => {
        const rows = qsa('[data-search-row]', rowsWrap)
        if (rows.length <= 1) {
          if (brandSelect) brandSelect.value = ''
          if (modelSelect) modelSelect.value = ''
          if (variantInput) variantInput.value = ''
          fillModels('', modelSelect, '')
          scheduleCount()
          return
        }
        row.remove()
        scheduleCount()
      })
    }

    const addRow = (initial = {}) => {
      if (!template || !rowsWrap) return
      const node = template.content.firstElementChild.cloneNode(true)
      rowsWrap.appendChild(node)
      bindRow(node, initial)
    }

    const buildLines = () => {
      const rows = qsa('[data-search-row]', rowsWrap)
      const lines = []
      rows.forEach((row) => {
        const brand = normalizeBrand(qs('[data-line-brand]', row)?.value || '')
        const model = qs('[data-line-model]', row)?.value || ''
        const variant = qs('[data-line-variant]', row)?.value || ''
        if (!brand && !model && !variant) return
        lines.push([brand, model, variant].map((v) => v.trim()).join('|'))
      })
      return lines
    }

    const buildParams = (withPaging) => {
      const data = new FormData(form)
      const params = new URLSearchParams()
      const lineKeys = ['line_brand', 'line_model', 'line_variant']
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
      if (!params.get('brand') && lines.length === 1) {
        const first = parseLine(lines[0])
        if (first.brand) params.set('brand', first.brand)
        if (first.model) params.set('model', first.model)
      }
      if (withPaging) {
        params.set('page', '1')
        params.set('page_size', '1')
      }
      return params
    }

    let debounce
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

    const renderSuggestions = async () => {
      if (!suggestionsEl) return
      suggestionsEl.innerHTML = ''
      const lines = buildLines()
      const first = lines.length ? parseLine(lines[0]) : { brand: '', model: '' }
      const params = new URLSearchParams()
      const country = qs('input[name="country"]', form)?.value
      if (country) params.set('country', country)
      if (first.brand) params.set('brand', normalizeBrand(first.brand))
      if (first.model) params.set('model', first.model)
      params.set('page', '1')
      params.set('page_size', '6')
      try {
        const fx = await getFx()
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
          card.innerHTML = `
            <div class="thumb-wrap">
              <img class="thumb" src="${thumb}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer" data-thumb="${thumb}" data-orig="${origThumb}" data-id="${car.id}" onerror="this.onerror=null;this.src='/static/img/no-photo.svg';" />
            </div>
            <div class="car-card__body">
              <div>
                <div class="car-card__title">${car.brand || ''} ${car.model || ''}</div>
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

    form.addEventListener('submit', () => {
      prepareSubmit()
      if (DEBUG_FILTERS) {
        const qs = new URLSearchParams(new FormData(form)).toString()
        console.info('filters:advanced submit', qs)
      }
    })

    form.addEventListener('reset', () => {
      setTimeout(() => {
        if (rowsWrap) rowsWrap.innerHTML = ''
        addRow({})
        bindRegionSelect(form)
        bindRegMonthState(form)
        bindOtherColorsToggle(form)
        syncColorChips(form)
        updateRegionFilters()
        scheduleCount()
      }, 0)
    })

    addBtn?.addEventListener('click', () => {
      addRow({})
      scheduleCount()
    })

    const linesFromUrl = new URLSearchParams(window.location.search).getAll('line')
    if (linesFromUrl.length) {
      linesFromUrl.forEach((line) => addRow(parseLine(line)))
    } else {
      addRow({})
    }
    if (form.id !== 'advanced-search-form') {
      bindRegionSelect(form)
    }
    updateRegionSub()
    updateRegionFilters()
    loadPayloadOptions()
    syncAdvancedGenerationVisibility()
    bindRegMonthState(form)
    bindColorChips(form, scheduleCount)
    bindOtherColorsToggle(form)
    const ctrls = qsa('input, select', form)
    ctrls.forEach((el) => {
      el.addEventListener('change', scheduleCount)
      el.addEventListener('input', scheduleCount)
    })
    regionSelect?.addEventListener('change', () => {
      updateRegionSub()
      loadPayloadOptions()
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

  function initDetailGallery() {
    const main = qs('.detail-hero__main')
    const img = qs('#primaryImage')
    if (!main || !img) return
    window.__detailGalleryHandled = true
    let images = []
    try {
      images = JSON.parse(main.dataset.images || '[]')
    } catch (e) {
      images = []
    }
    if (!Array.isArray(images) || images.length < 2) return
    images = images.map((u) => normalizeThumbUrl(u)).filter((u) => u && u !== '/static/img/no-photo.svg')
    if (images.length < 2) return
    let idx = Math.max(0, images.indexOf(img.getAttribute('src')))
    const setImage = (nextIdx) => {
      idx = (nextIdx + images.length) % images.length
      const nextSrc = images[idx]
      if (nextSrc && nextSrc !== '/static/img/no-photo.svg') {
        img.src = nextSrc
      }
    }
    if (!img.dataset.fallbackBound) {
      img.dataset.fallbackBound = '1'
      img.onerror = () => {
        if (img.dataset.fallbackApplied === '1') return
        img.dataset.fallbackApplied = '1'
        img.src = '/static/img/no-photo.svg'
      }
    }
    const prevBtn = qs('[data-detail-prev]')
    const nextBtn = qs('[data-detail-next]')
    prevBtn?.addEventListener('click', (e) => {
      e.preventDefault()
      setImage(idx - 1)
    })
    nextBtn?.addEventListener('click', (e) => {
      e.preventDefault()
      setImage(idx + 1)
    })
    const thumbs = qsa('.detail-hero__thumbs .thumb')
    thumbs.forEach((btn, i) => {
      btn.addEventListener('click', () => {
        idx = i
        const nextSrc = normalizeThumbUrl(images[idx] || '')
        if (nextSrc) img.src = nextSrc
      })
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
        setImage(diff > 0 ? idx - 1 : idx + 1)
      }
      startX = null
    })
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
    initNav()
    loadFavoritesState()
    initCatalog()
    initHome()
    initAdvancedSearch()
    initDetailGallery()
    initDetailActions()
    applyLeadPrefill()
    initLeadFromDetail()
    initBackToTop()
    initThumbFallbacks()
    convertInlinePrices()
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initAll)
  } else {
    initAll()
  }
  window.LA_APP_INIT = initAll
})()
