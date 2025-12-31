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
      const field = form.elements[key]
      if (!field) return
      if (field.tagName === 'SELECT' || field.tagName === 'INPUT') {
        field.value = value
      }
    })
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
      color: 'Цвет',
      body_type: 'Кузов',
      engine_type: 'Топливо',
      transmission: 'КПП',
      price_min: 'Цена от',
      price_max: 'Цена до',
      year_min: 'Год от',
      year_max: 'Год до',
      mileage_max: 'Пробег до',
      source: 'Источник',
    }
    const chips = []
    params.forEach((value, key) => {
      if (!value || ['page', 'page_size'].includes(key)) return
      const label = labels[key] || key
      chips.push({ key, label, value })
    })
    if (!chips.length) {
      container.innerHTML = '<span class="muted">Фильтры не выбраны</span>'
      return
    }
    container.innerHTML = ''
    chips.forEach(({ key, label, value }) => {
      const chip = document.createElement('button')
      chip.type = 'button'
      chip.className = 'filter-chip'
      chip.innerHTML = `<span>${label}: ${value}</span><span class="chip-close">×</span>`
      chip.addEventListener('click', () => {
        const el = form.elements[key]
        if (el) el.value = ''
        if (key === 'model') qs('#catalog-search') && (qs('#catalog-search').value = '')
        loadCars(1)
      })
      container.appendChild(chip)
    })
  }

  function collectParams(page) {
    const form = qs('#filters')
    const data = new FormData(form)
    const params = new URLSearchParams()
    const numericKeys = ['price_min', 'price_max', 'year_min', 'year_max', 'mileage_max']
    for (const [k, v] of data.entries()) {
      if (!v) continue
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
      qs('#filters')?.reset()
      qs('#catalog-search') && (qs('#catalog-search').value = '')
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
    const featuredBlock = qs('#featuredBlock')
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
      // hide featured block if any filters applied
      if (featuredBlock) {
        const hasFilters = Array.from(params.entries()).some(
          ([k, v]) => v && !['page', 'page_size'].includes(k)
        )
        featuredBlock.style.display = hasFilters ? 'none' : ''
      }
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
        const more = (car.images_count && car.images_count > 1 && car.thumbnail_url) ? `<span class="more-badge">+${car.images_count - 1} фото</span>` : ''
        const price = car.price != null ? formatPrice(car.price, car.currency, fx) : ''
        const metaLine = [car.year, car.engine_type].filter(Boolean).join(' · ')
        const colorDot = (clr) => {
          if (!clr) return ''
          const safe = clr.toLowerCase()
          const palette = {
            black: '#111',
            white: '#f5f5f5',
            gray: '#888',
            grey: '#888',
            silver: '#c0c0c0',
            red: '#d82424',
            blue: '#2d7dd2',
            green: '#1f9d55',
            yellow: '#f9c846',
            orange: '#f97316',
            brown: '#9c6b3c',
            beige: '#d9c6a5',
            purple: '#8b5cf6',
            violet: '#7c3aed',
            gold: '#d4af37',
          }
          const key = Object.keys(palette).find((k) => safe.includes(k))
          const val = key ? palette[key] : clr
          return `<span class="spec-dot" style="background:${val}"></span>`
        }
        const specLines = []
        if (car.mileage != null) {
          specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/mileage.svg" alt="">${Number(car.mileage).toLocaleString('ru-RU')} км</span>`)
        }
        if (car.engine_type) {
          specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/fuel.svg" alt="">${car.engine_type}</span>`)
        }
        if (car.color) {
          specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/color.svg" alt="">${colorDot(car.color)}${car.color}</span>`)
        }
        if (car.country) {
          specLines.push(`<span class="spec-line"><img class="spec-icon" src="/static/img/icons/flag.svg" alt="">${car.country}</span>`)
        }
        card.innerHTML = `
          <div class="thumb-wrap">
            <img class="thumb" src="${car.thumbnail_url || ''}" alt="" loading="lazy"/>
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
    applyQueryToFilters()
    const urlParams = new URLSearchParams(window.location.search)
    const initialPage = Number(urlParams.get('page') || 1)
    const initialModelParam = urlParams.get('model') || ''
    const searchField = qs('#catalog-search')
    const modelSelect = qs('#model-select')
    const brandSelect = qs('#brand')
    const colorSelect = qs('#colorSelect')
    const colorChips = qsa('#colorSwatches .color-chip')
    qs('#applyFilters')?.addEventListener('click', (e) => {
      e.preventDefault()
      loadCars(1)
    })
    searchField?.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault()
        loadCars(1)
      }
    })
    qs('#catalogSearchBtn')?.addEventListener('click', () => {
      loadCars(1)
    })
    qs('#resetFilters')?.addEventListener('click', (e) => {
      e.preventDefault()
      const form = qs('#filters')
      form?.reset()
      if (searchField) searchField.value = ''
      colorChips.forEach((chip) => chip.classList.remove('active'))
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
    }
    const toggle = qs('#filtersToggle')
    const panel = qs('#filtersPanel')
    toggle?.addEventListener('click', () => {
      panel?.classList.toggle('open')
    })

    // color chips: sync with select and query
    if (colorChips.length && colorSelect) {
      colorChips.forEach((chip) => {
        const hex = chip.dataset.hex
        if (hex) chip.style.background = hex
        if (chip.dataset.label) chip.title = chip.dataset.label
      })
      const setActive = (val) => {
        colorChips.forEach((chip) => {
          chip.classList.toggle('active', chip.dataset.value === val)
        })
      }
      setActive(colorSelect.value || '')
      colorChips.forEach((chip) => {
        chip.addEventListener('click', () => {
          const val = chip.dataset.value || ''
          colorSelect.value = val
          setActive(val)
          loadCars(1)
        })
      })
      colorSelect.addEventListener('change', () => setActive(colorSelect.value || ''))
    }
    async function updateCatalogModels() {
      if (!brandSelect || !modelSelect) return
      const brand = brandSelect.value
      modelSelect.innerHTML = ''
      if (!brand) {
        modelSelect.disabled = false
        modelSelect.innerHTML = '<option value=\"\">Все</option>'
        return
      }
      modelSelect.disabled = true
      modelSelect.innerHTML = '<option value=\"\">Загрузка…</option>'
      const models = await fetchModels(brand)
      modelSelect.innerHTML = '<option value=\"\">Все</option>'
      models.forEach(({ model }) => {
        const opt = document.createElement('option')
        opt.value = model
        opt.textContent = model
        modelSelect.appendChild(opt)
      })
      if (initialModelParam) {
        modelSelect.value = initialModelParam
      }
      modelSelect.disabled = false
    }
    brandSelect?.addEventListener('change', () => {
      updateCatalogModels().then(() => loadCars(1))
    })
    loadCars(initialPage)
    if (brandSelect && brandSelect.value) {
      updateCatalogModels()
    }
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
      const res = await fetch(`/api/brands/${encodeURIComponent(brand)}/models`)
      const data = await res.json()
      return data.models || []
    } catch (e) {
      console.error('models', e)
      return []
    }
  }

  function initHome() {
    const form = qs('#home-search')
    if (!form) return
    const brandSelect = qs('#home-brand')
    const modelSelect = qs('#home-model')
    const resetBtn = qs('#home-reset')

    async function updateModels() {
      if (!brandSelect || !modelSelect) return
      const brand = brandSelect.value
      modelSelect.innerHTML = ''
      if (!brand) {
        modelSelect.disabled = true
        modelSelect.innerHTML = '<option value=\"\">Сначала выберите марку</option>'
        return
      }
      modelSelect.disabled = true
      modelSelect.innerHTML = '<option value=\"\">Загрузка…</option>'
      const models = await fetchModels(brand)
      modelSelect.innerHTML = '<option value=\"\">Любая</option>'
      models.forEach(({ model }) => {
        const opt = document.createElement('option')
        opt.value = model
        opt.textContent = model
        modelSelect.appendChild(opt)
      })
      modelSelect.disabled = false
    }

    async function loadBrands() {
      const brands = await fetchBrands()
      if (!brandSelect) return
      if (brands && Array.isArray(brands) && brands.length) {
        brandSelect.innerHTML = '<option value=\"\">Любая</option>'
        const names = brands
          .map((b) => b.brand || b)
          .filter(Boolean)
          .sort((a, b) => a.localeCompare(b))
        names.forEach((name) => {
          const opt = document.createElement('option')
          opt.value = name
          opt.textContent = name
          brandSelect.appendChild(opt)
        })
      }
    }

    brandSelect?.addEventListener('change', updateModels)

    if (resetBtn) {
      resetBtn.addEventListener('click', (e) => {
        e.preventDefault()
        form.reset()
        if (modelSelect) {
          modelSelect.innerHTML = '<option value=\"\">Сначала выберите марку</option>'
          modelSelect.disabled = true
        }
      })
    }

    loadBrands().then(() => {
      if (brandSelect && brandSelect.value) {
        updateModels()
      }
    })
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

  function initAll() {
    initNav()
    loadFavoritesState()
    initCatalog()
    initHome()
    applyLeadPrefill()
    initLeadFromDetail()
    convertInlinePrices()
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initAll)
  } else {
    initAll()
  }
  window.LA_APP_INIT = initAll
})()
