;(function () {
  const qs = (s, el = document) => el.querySelector(s)
  const qsa = (s, el = document) => Array.from(el.querySelectorAll(s))

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
    const rounded = Math.ceil(Number(val))
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
    const toggle = qs('#colorsToggle', scope)
    const extra = qs('#colorsExtra', scope)
    const input = qs('input[name="color"]', scope)
    if (!toggle || !extra) return
    const update = (expanded) => {
      toggle.setAttribute('aria-expanded', expanded ? 'true' : 'false')
      extra.classList.toggle('is-collapsed', !expanded)
      toggle.textContent = expanded ? 'Скрыть цвета' : 'Другие цвета'
    }
    if (!toggle.dataset.bound) {
      toggle.addEventListener('click', () => {
        const expanded = toggle.getAttribute('aria-expanded') === 'true'
        update(!expanded)
      })
      toggle.dataset.bound = '1'
    }
    if (input?.value) {
      const hasInExtra = extra.querySelector(`.color-chip[data-color="${input.value}"]`)
      if (hasInExtra) {
        update(true)
        return
      }
    }
    update(false)
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
    const euSelect = qs('[data-eu-country]', scope)
    const krSelect = qs('[data-kr-type]', scope)
    const countryHidden = qs('input[name="country"]', scope)
    if (!region || !countryHidden) return
    if (!region.value && countryHidden.value) {
      region.value = countryHidden.value === 'KR' ? 'KR' : 'EU'
    }
    const update = () => {
      const val = region.value
      if (euPanel) euPanel.classList.toggle('is-hidden', val !== 'EU')
      if (krPanel) krPanel.classList.toggle('is-hidden', val !== 'KR')
      if (val === 'EU') {
        const euVal = euSelect?.value || ''
        countryHidden.value = euVal || 'EU'
      } else if (val === 'KR') {
        countryHidden.value = 'KR'
      } else {
        countryHidden.value = ''
      }
    }
    region.addEventListener('change', update)
    euSelect?.addEventListener('change', update)
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
      eu_country: null,
      kr_type: null,
      interior_color: null,
      interior_material: null,
      air_suspension: null,
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
    const skipKeys = ['region', 'eu_country', 'kr_type']
    for (const [k, v] of data.entries()) {
      if (!v) continue
      if (skipKeys.includes(k)) continue
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
    params.set('page', String(page || 1))
    params.set('page_size', '12')
    const pageField = form.querySelector('input[name="page"]')
    if (pageField) pageField.value = String(page || 1)
    return params
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

  async function loadFavoritesState() {
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

  async function loadCars(page = 1) {
    const spinner = qs('#spinner')
    const cards = qs('#cards')
    const pageInfo = qs('#pageInfo')
    const resultCount = qs('#resultCount')
    if (!spinner || !cards) return
    spinner.style.display = 'block'
    renderSkeleton(cards)
    try {
      const fx = await getFx()
      const params = collectParams(page)
      const res = await fetch(`${window.CATALOG_API}?${params.toString()}`)
      if (!res.ok) {
        throw new Error(`API ${res.status}`)
      }
      const data = await res.json()
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

      cards.innerHTML = ''
      if (!Array.isArray(data.items) || !data.items.length) {
        renderEmpty(cards)
        return
      }

      for (const car of data.items) {
        const card = document.createElement('a')
        card.href = `/car/${car.id}`
        card.className = 'car-card'
        const images = Array.isArray(car.images) && car.images.length ? car.images : (car.thumbnail_url ? [car.thumbnail_url] : [])
        const hasGallery = images.length > 1
        const thumbSrc = images[0] || ''
        const navControls = hasGallery
          ? `
            <button class="thumb-nav thumb-nav--prev" type="button" aria-label="Предыдущее фото">‹</button>
            <button class="thumb-nav thumb-nav--next" type="button" aria-label="Следующее фото">›</button>
          `
          : ''
        const more = (car.images_count && car.images_count > 1 && car.thumbnail_url) ? `<span class="more-badge">+${car.images_count - 1} фото</span>` : ''
        const price = car.price != null ? formatPrice(car.price, car.currency, fx) : ''
        const metaLine = [car.year, car.display_engine_type || car.engine_type].filter(Boolean).join(' · ')
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
            <div class="car-card__price">${price}</div>
          </div>
        `
        const img = card.querySelector('img.thumb')
        const wrap = card.querySelector('.thumb-wrap')
        if (wrap) wrap.classList.add('thumb-loading')
        if (img) {
          img.style.opacity = '0'
          img.addEventListener('load', () => {
            img.style.opacity = '1'
            if (wrap) wrap.classList.remove('thumb-loading')
          })
          img.addEventListener('error', () => {
            img.style.opacity = '1'
            if (wrap) wrap.classList.remove('thumb-loading')
          })
        }
        if (hasGallery && img && wrap) {
          let idx = 0
          const setImage = (nextIdx) => {
            idx = (nextIdx + images.length) % images.length
            img.src = images[idx]
          }
          const prevBtn = wrap.querySelector('.thumb-nav--prev')
          const nextBtn = wrap.querySelector('.thumb-nav--next')
          prevBtn?.addEventListener('click', (e) => {
            e.preventDefault()
            e.stopPropagation()
            setImage(idx - 1)
          })
          nextBtn?.addEventListener('click', (e) => {
            e.preventDefault()
            e.stopPropagation()
            setImage(idx + 1)
          })
          let startX = null
          wrap.addEventListener('touchstart', (e) => {
            if (e.touches.length) startX = e.touches[0].clientX
          })
          wrap.addEventListener('touchend', (e) => {
            if (startX == null) return
            const endX = e.changedTouches[0].clientX
            const diff = endX - startX
            if (Math.abs(diff) > 30) {
              setImage(diff > 0 ? idx - 1 : idx + 1)
            }
            startX = null
          })
        }
        cards.appendChild(card)
      }
      bindFavoriteButtons(cards)
      window.__page = data.page
      window.__pageSize = data.page_size
      window.__total = data.total
    } catch (e) {
      console.error(e)
      if (pageInfo) pageInfo.textContent = 'Ошибка загрузки'
      if (cards) renderEmpty(cards, 'Не удалось загрузить данные. Попробуйте позже.')
    } finally {
      spinner.style.display = 'none'
    }
  }

  function initCatalog() {
    if (!qs('#cards')) return
    // restore scroll position when coming back from detail
    const saved = sessionStorage.getItem('catalogScroll')
    if (saved) {
      requestAnimationFrame(() => {
        window.scrollTo({ top: Number(saved) || 0, behavior: 'instant' })
      })
      sessionStorage.removeItem('catalogScroll')
    }
    applyQueryToFilters()
    const urlParams = new URLSearchParams(window.location.search)
    const initialPage = Number(urlParams.get('page') || 1)
    const initialModelParam = urlParams.get('model') || ''
    const initialSort = urlParams.get('sort') || 'price_asc'
    const modelSelect = qs('#model-select')
    const brandSelect = qs('#brand')
    normalizeBrandOptions(brandSelect)
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
    qs('#applyFilters')?.addEventListener('click', (e) => {
      e.preventDefault()
      sessionStorage.setItem('catalogScroll', String(window.scrollY))
      loadCars(1)
    })
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
    // auto-apply filters on change / typing
    const filtersForm = qs('#filters')
    if (filtersForm) {
      bindColorChips(filtersForm, () => loadCars(1))
      bindOtherColorsToggle(filtersForm)
      bindRegMonthState(filtersForm)
      bindRegionSelect(filtersForm)
      const ctrls = qsa('input, select', filtersForm)
      let debounce
      const trigger = () => {
        clearTimeout(debounce)
        debounce = setTimeout(() => {
          loadCars(1)
        }, 250)
      }
      ctrls.forEach((el) => {
        el.addEventListener('change', trigger)
        el.addEventListener('input', trigger)
      })
      // apply initial sort/generation if present
      const sortSelect = qs('#sortHidden', filtersForm)
      if (sortSelect && initialSort) sortSelect.value = initialSort
      const sortTopbar = qs('#sort-select')
      if (sortTopbar && initialSort) sortTopbar.value = initialSort
      if (sortTopbar) {
        sortTopbar.addEventListener('change', () => {
          const val = sortTopbar.value
          if (sortSelect) sortSelect.value = val
          loadCars(1)
        })
      }
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
        modelSelect.innerHTML = '<option value=\"\">Все</option>'
        return
      }
      modelSelect.disabled = true
      modelSelect.innerHTML = '<option value=\"\">Загрузка…</option>'
      const models = await fetchModels(normBrand)
      modelSelect.innerHTML = '<option value=\"\">Все</option>'
      models.forEach(({ model }) => {
        const opt = document.createElement('option')
        opt.value = model
        opt.textContent = model
        modelSelect.appendChild(opt)
      })
      if (initialModelParam) {
        setSelectValueInsensitive(modelSelect, initialModelParam)
      }
      modelSelect.disabled = false
    }
    brandSelect?.addEventListener('change', () => {
      updateCatalogModels().then(() => loadCars(1))
    })
    const loadInitial = async () => {
      if (brandSelect && brandSelect.value) {
        await updateCatalogModels()
      }
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

  async function fetchModels(brand) {
    if (!brand) return []
    try {
      const normalized = normalizeBrand(brand)
      const res = await fetch(`/api/brands/${encodeURIComponent(normalized)}/models`)
      const data = await res.json()
      return data.models || []
    } catch (e) {
      console.error('models', e)
      return []
    }
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
    const countEl = qs('#home-count')
    const countTopEl = qs('#home-count-top')
    normalizeBrandOptions(brandSelect)

    if (resetBtn) {
      resetBtn.addEventListener('click', (e) => {
        e.preventDefault()
        form.reset()
        bindRegionSelect(form)
        if (modelSelect) {
          modelSelect.innerHTML = '<option value="">Все</option>'
          modelSelect.disabled = true
        }
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
      const models = await fetchModels(brand)
      modelSelect.innerHTML = '<option value="">Все</option>'
      models.forEach(({ model }) => {
        const opt = document.createElement('option')
        opt.value = model
        opt.textContent = model
        modelSelect.appendChild(opt)
      })
      modelSelect.disabled = false
    }

    brandSelect?.addEventListener('change', () => {
      updateHomeModels().then(() => updateCount())
    })

    function buildHomeParams(withPaging = false) {
      const data = new FormData(form)
      const params = new URLSearchParams()
      const numericKeys = ['price_min', 'price_max', 'mileage_min', 'mileage_max']
      for (const [k, v] of data.entries()) {
        if (!v) continue
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
      if (withPaging) {
        params.set('page', '1')
        params.set('page_size', '1')
      }
      return params
    }

    let debounce
    const updateCount = () => {
      if (!countEl && !countTopEl) return
      clearTimeout(debounce)
      debounce = setTimeout(async () => {
        try {
          const params = buildHomeParams(true)
          const res = await fetch(`/api/cars?${params.toString()}`)
          if (!res.ok) return
          const data = await res.json()
          if (countEl) animateCount(countEl, data.total || 0)
          if (countTopEl) animateCount(countTopEl, data.total || 0)
          if (DEBUG_FILTERS) {
            sessionStorage.setItem('homeCountParams', params.toString())
          }
        } catch (e) {
          console.warn('home count', e)
        }
      }, 300)
    }

    const ctrls = qsa('input, select', form)
    ctrls.forEach((el) => {
      el.addEventListener('change', updateCount)
      el.addEventListener('input', updateCount)
    })
    form.addEventListener('submit', (e) => {
      e.preventDefault()
      const params = buildHomeParams(false)
      if (DEBUG_FILTERS) {
        sessionStorage.setItem('homeSubmitParams', params.toString())
      }
      window.location.href = `/catalog?${params.toString()}`
    })
    bindRegionSelect(form)
    updateCount()
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
    const regionEuSelect = qs('[data-eu-country]', form)
    const regionKrSelect = qs('[data-kr-type]', form)

    const parseOptions = (raw) => {
      try {
        const data = JSON.parse(raw || '[]')
        return Array.isArray(data) ? data : []
      } catch {
        return []
      }
    }

    const setSelectOptions = (select, options) => {
      if (!select) return
      const current = select.value
      const deduped = []
      const seen = new Set()
      options.forEach((val) => {
        const key = String(val)
        if (key && !seen.has(key)) {
          seen.add(key)
          deduped.push(key)
        }
      })
      select.innerHTML = ''
      const emptyOpt = document.createElement('option')
      emptyOpt.value = ''
      emptyOpt.textContent = 'Не важно'
      select.appendChild(emptyOpt)
      deduped.forEach((val) => {
        const opt = document.createElement('option')
        opt.value = val
        opt.textContent = val
        select.appendChild(opt)
      })
      if (current && deduped.includes(current)) {
        select.value = current
      } else {
        select.value = ''
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
        setSelectOptions(select, next)
      })
    }

    const updateRegionSub = () => {
      const region = regionSelect?.value || ''
      const hasKr = Boolean(regionSubKr)
      const showKr = region === 'KR' && hasKr
      const showEu = !showKr
      if (regionSubEu) {
        regionSubEu.classList.toggle('is-hidden-keep', !showEu)
      }
      if (regionSubKr) {
        regionSubKr.classList.toggle('is-hidden-keep', !showKr)
      }
      if (regionEuSelect) {
        regionEuSelect.disabled = region !== 'EU'
      }
      if (regionKrSelect) {
        regionKrSelect.disabled = !showKr
      }
    }

    const parseLine = (line) => {
      const parts = String(line || '').split('|')
      return {
        brand: normalizeBrand(parts[0] || ''),
        model: (parts[1] || '').trim(),
        variant: (parts[2] || '').trim(),
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
      const models = await fetchModels(brand)
      modelSelect.innerHTML = '<option value="">Неважно</option>'
      models.forEach(({ model }) => {
        const opt = document.createElement('option')
        opt.value = model
        opt.textContent = model
        modelSelect.appendChild(opt)
      })
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
      const skipKeys = ['region', 'eu_country', 'kr_type']
      const lineKeys = ['line_brand', 'line_model', 'line_variant']
      for (const [k, v] of data.entries()) {
        if (!v) continue
        if (skipKeys.includes(k)) continue
        if (lineKeys.includes(k)) continue
        params.append(k, v)
      }
      buildLines().forEach((line) => params.append('line', line))
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
      const res = await fetch(`/api/cars?${params.toString()}`)
      if (!res.ok) return
      const data = await res.json()
          animateCount(countEl, data.total || 0)
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
          const thumb = car.thumbnail_url || (Array.isArray(car.images) ? car.images[0] : '') || ''
          const price = car.price != null ? formatPrice(car.price, car.currency, fx) : ''
          card.innerHTML = `
            <div class="thumb-wrap">
              <img class="thumb" src="${thumb}" alt="" loading="lazy" decoding="async" />
            </div>
            <div class="car-card__body">
              <div>
                <div class="car-card__title">${car.brand || ''} ${car.model || ''}</div>
              </div>
              <div class="car-card__price">${price}</div>
            </div>
          `
          grid.appendChild(card)
        })
        suggestionsEl.appendChild(grid)
      } catch (e) {
        console.warn('suggestions', e)
      }
    }

    form.addEventListener('submit', async (e) => {
      e.preventDefault()
      if (messageEl) messageEl.textContent = ''
      if (suggestionsEl) suggestionsEl.innerHTML = ''
      try {
        const params = buildParams(true)
        const res = await fetch(`/api/cars?${params.toString()}`)
        if (!res.ok) return
        const data = await res.json()
        if (data.total && data.total > 0) {
          const redirectParams = buildParams(false)
          window.location.href = `/catalog?${redirectParams.toString()}`
          return
        }
        if (messageEl) {
          messageEl.textContent = 'Таких машин не найдено. Попробуйте изменить фильтры.'
        }
        renderSuggestions()
      } catch (e) {
        console.warn('advanced search', e)
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
    bindRegionSelect(form)
    updateRegionSub()
    updateRegionFilters()
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
      updateRegionFilters()
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
    let images = []
    try {
      images = JSON.parse(main.dataset.images || '[]')
    } catch (e) {
      images = []
    }
    if (!Array.isArray(images) || images.length < 2) return
    let idx = Math.max(0, images.indexOf(img.getAttribute('src')))
    const setImage = (nextIdx) => {
      idx = (nextIdx + images.length) % images.length
      img.src = images[idx]
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
        img.src = images[idx]
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

  function initAll() {
    initNav()
    loadFavoritesState()
    initCatalog()
    initHome()
    initAdvancedSearch()
    initDetailGallery()
    applyLeadPrefill()
    initLeadFromDetail()
    initBackToTop()
    convertInlinePrices()
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initAll)
  } else {
    initAll()
  }
  window.LA_APP_INIT = initAll
})()
