/* Level Avto admin — minimal UX glue.
 * - Sidebar toggle (persisted in localStorage)
 * - Toast helper (window.adminToast)
 * - Confirm-on-submit forms (data-confirm)
 * - Auto-dismiss URL flash messages (?flash=…)
 */
(function () {
  'use strict'

  const SIDEBAR_KEY = 'la-admin:sidebar'

  function applySidebarState(state) {
    const shell = document.querySelector('.la-admin-shell')
    if (!shell) return
    shell.dataset.sidebar = state
  }

  function initSidebar() {
    const shell = document.querySelector('.la-admin-shell')
    if (!shell) return
    const stored = localStorage.getItem(SIDEBAR_KEY)
    if (stored === 'collapsed' || stored === 'open') applySidebarState(stored)
    document.addEventListener('click', (event) => {
      const btn = event.target.closest('[data-sidebar-toggle]')
      if (!btn) return
      event.preventDefault()
      const current = shell.dataset.sidebar || ''
      const next = current === 'collapsed' ? 'open' : 'collapsed'
      shell.dataset.sidebar = next
      try {
        localStorage.setItem(SIDEBAR_KEY, next)
      } catch (e) {
        // storage may be blocked; ignore.
      }
    })
  }

  function ensureToastHost() {
    let host = document.querySelector('.la-toasts')
    if (host) return host
    host = document.createElement('div')
    host.className = 'la-toasts'
    document.body.appendChild(host)
    return host
  }

  function showToast(message, options) {
    const opts = options || {}
    const host = ensureToastHost()
    const node = document.createElement('div')
    node.className = 'la-toast'
    if (opts.kind === 'success') node.classList.add('la-toast--success')
    if (opts.kind === 'danger' || opts.kind === 'error') node.classList.add('la-toast--danger')
    node.textContent = message
    host.appendChild(node)
    const ttl = typeof opts.ttl === 'number' ? opts.ttl : 4000
    if (ttl > 0) {
      setTimeout(() => {
        node.style.transition = 'opacity 0.2s ease'
        node.style.opacity = '0'
        setTimeout(() => node.remove(), 220)
      }, ttl)
    }
    return node
  }

  function initFlashFromQuery() {
    try {
      const params = new URLSearchParams(window.location.search)
      const flash = params.get('flash')
      const flashError = params.get('flash_error')
      if (flash) {
        showToast(decodeURIComponent(flash), { kind: 'success' })
      }
      if (flashError) {
        showToast(decodeURIComponent(flashError), { kind: 'danger', ttl: 6000 })
      }
      if (flash || flashError) {
        params.delete('flash')
        params.delete('flash_error')
        const next = window.location.pathname + (params.toString() ? '?' + params.toString() : '')
        window.history.replaceState({}, document.title, next)
      }
    } catch (e) {
      // no-op
    }
  }

  function initConfirmForms() {
    document.addEventListener('submit', (event) => {
      const form = event.target.closest('form[data-confirm]')
      if (!form) return
      const message = form.dataset.confirm || 'Подтвердить действие?'
      if (!window.confirm(message)) {
        event.preventDefault()
        event.stopPropagation()
      }
    })
  }

  function initActiveNav() {
    const path = window.location.pathname
    const hash = window.location.hash || ''
    document.querySelectorAll('.la-admin-nav__item').forEach((link) => {
      const target = link.getAttribute('href') || ''
      if (!target) return
      const targetPath = target.split('#')[0]
      const targetHash = target.includes('#') ? '#' + target.split('#').slice(1).join('#') : ''
      if (targetPath !== path) return
      if (targetHash) {
        if (hash && hash === targetHash) link.classList.add('is-active')
        return
      }
      if (path === '/admin' && !hash) {
        link.classList.add('is-active')
        return
      }
      if (target !== '/admin' && path.startsWith(targetPath)) {
        link.classList.add('is-active')
      }
    })
  }

  function activateTabFromHash() {
    const hash = window.location.hash || ''
    const match = /^#tab=([a-zA-Z0-9_-]+)/.exec(hash)
    if (!match) return
    const key = match[1]
    const tabs = document.querySelectorAll('.la-tab')
    if (!tabs.length) return
    let found = false
    tabs.forEach((tab) => {
      const isMatch = tab.dataset.tab === key
      tab.classList.toggle('is-active', isMatch)
      if (isMatch) found = true
    })
    document.querySelectorAll('.la-tab-panel').forEach((panel) => {
      panel.hidden = panel.dataset.tabPanel !== key
    })
    if (found) {
      const card = document.querySelector('.la-tabs')
      if (card && typeof card.scrollIntoView === 'function') {
        card.scrollIntoView({ behavior: 'smooth', block: 'start' })
      }
    }
  }

  /* ──────────────────────────────────────────────────────────────────
     Cars picker — typeahead used in the notifications composer and in
     the "featured cars" form. Auto-attaches to any element with
     ``data-cars-picker`` so templates only need to drop the markup.
     ────────────────────────────────────────────────────────────────── */
  function initCarsPickers() {
    document.querySelectorAll('[data-cars-picker]').forEach((root) => {
      if (root.dataset.carsPickerReady === '1') return
      root.dataset.carsPickerReady = '1'

      const input = root.querySelector('.la-cars-picker__search')
      const sugg = root.querySelector('.la-cars-picker__suggestions')
      const chipsBox = root.querySelector('[data-cars-picker-chips]')
      const hidden = root.querySelector('[data-cars-picker-input]')
      if (!input || !sugg || !chipsBox || !hidden) return

      // Optional id-add subform — second input + ‹+› button. Supported by
      // newer templates that want explicit "I know the id, just take it"
      // affordance on top of the typeahead.
      const idInput = root.querySelector('[data-cars-picker-id-input]')
      const idAddBtn = root.querySelector('[data-cars-picker-id-add]')

      // Optional cap on how many cars can sit in the list at once.
      // Driven by data-cars-picker-limit on the picker root. Falls back
      // to a permissive default so legacy templates aren't constrained.
      const limit = parseInt(root.dataset.carsPickerLimit || '0', 10) || 0

      // Counter element lives in the field hint outside the picker root,
      // so look it up at the document level. Optional — older templates
      // can omit the marker and the picker will still work.
      const counterEl = document.querySelector('[data-cars-picker-counter]')

      // Seed selected map from any prefilled chips left by the server.
      const selected = new Map()
      chipsBox.querySelectorAll('.la-chip[data-prefilled]').forEach((chip) => {
        const id = chip.dataset.prefilled
        if (id) selected.set(String(id), { title: '' })
      })

      function syncHidden() {
        hidden.value = Array.from(selected.keys()).join(',')
        if (counterEl) counterEl.textContent = String(selected.size)
        // Lock the inputs once the operator has hit the configured cap —
        // keeps the UX honest instead of failing on the server with a
        // truncation message.
        const atCap = limit > 0 && selected.size >= limit
        if (input) input.disabled = atCap
        if (idInput) idInput.disabled = atCap
        if (idAddBtn) idAddBtn.disabled = atCap
        if (atCap && counterEl && counterEl.parentElement) {
          counterEl.parentElement.dataset.atCap = '1'
        } else if (counterEl && counterEl.parentElement) {
          delete counterEl.parentElement.dataset.atCap
        }
      }

      function addChip(id, info) {
        if (selected.has(String(id))) return
        if (limit > 0 && selected.size >= limit) {
          window.alert('Максимум ' + limit + ' машин в списке. Удалите ненужные перед добавлением новых.')
          return
        }
        selected.set(String(id), info || { title: '' })
        const chip = document.createElement('span')
        chip.className = 'la-chip la-chip--with-action'
        chip.dataset.carId = String(id)
        chip.innerHTML = '<span>#' + id + ' ' + (info && info.title ? info.title : '') + '</span>'
        const rm = document.createElement('button')
        rm.type = 'button'
        rm.className = 'la-chip__remove'
        rm.textContent = '×'
        rm.addEventListener('click', () => {
          selected.delete(String(id))
          chip.remove()
          syncHidden()
        })
        chip.appendChild(rm)
        chipsBox.appendChild(chip)
        syncHidden()
      }

      // Add-by-id mini-form. We hit the same /search API that powers the
      // typeahead, but the server now resolves both the internal Car.id
      // and the mobile.de external_id, so an operator can paste either.
      function addById() {
        if (!idInput) return
        const raw = (idInput.value || '').trim()
        if (!raw) return
        if (!/^\d+$/.test(raw)) {
          window.alert('Введите число — id машины из каталога.')
          return
        }
        // Lookup via the admin search API — server resolves both Car.id
        // and external_id, so we just take the first exact match.
        fetch('/admin/api/cars/search?q=' + encodeURIComponent(raw) + '&limit=5', {
          credentials: 'same-origin',
        })
          .then((r) => {
            if (r.status === 401 || r.status === 403) {
              throw new Error('Сессия истекла. Перезайдите в админку.')
            }
            if (!r.ok) throw new Error('HTTP ' + r.status)
            return r.json()
          })
          .then((data) => {
            const results = data.results || []
            // Prefer match by internal id, then by external_id, otherwise
            // (safety net) take the single result that came back.
            let hit = results.find((it) => String(it.id) === raw)
            if (!hit) hit = results.find((it) => String(it.external_id || '') === raw)
            if (!hit && results.length === 1) hit = results[0]
            if (!hit) {
              window.alert('Машина #' + raw + ' не найдена в каталоге. Проверьте id.')
              return
            }
            const internalId = String(hit.id)
            if (selected.has(internalId)) {
              idInput.value = ''
              window.alert('Машина #' + internalId + ' уже в списке.')
              return
            }
            if (limit > 0 && selected.size >= limit) {
              window.alert('Максимум ' + limit + ' машин в списке.')
              return
            }
            addChip(hit.id, { title: hit.title || '', subtitle: hit.subtitle || '' })
            idInput.value = ''
            idInput.focus()
          })
          .catch((err) => {
            const msg = (err && err.message) ? err.message : 'Не удалось проверить id. Попробуйте позже.'
            window.alert(msg)
          })
      }
      if (idAddBtn) {
        idAddBtn.addEventListener('click', (event) => {
          event.preventDefault()
          addById()
        })
      }
      if (idInput) {
        idInput.addEventListener('keydown', (event) => {
          if (event.key === 'Enter') {
            event.preventDefault()
            addById()
          }
        })
      }

      // Wire up any × on prefilled chips (server-rendered).
      chipsBox.querySelectorAll('.la-chip[data-prefilled]').forEach((chip) => {
        const rm = chip.querySelector('.la-chip__remove')
        if (!rm || rm.dataset.bound === '1') return
        rm.dataset.bound = '1'
        rm.addEventListener('click', () => {
          const id = chip.dataset.prefilled
          if (id) selected.delete(String(id))
          chip.remove()
          syncHidden()
        })
      })
      syncHidden()

      function showSuggestions(items) {
        sugg.innerHTML = ''
        if (!items || !items.length) {
          sugg.hidden = true
          return
        }
        items.forEach((item) => {
          const row = document.createElement('button')
          row.type = 'button'
          row.className = 'la-cars-picker__row'
          row.innerHTML =
            '<img src="' + (item.thumbnail_url || '/static/img/no-photo.svg') + '" alt="">' +
            '<span class="la-cars-picker__title">' +
            '<strong>#' + item.id + ' · ' + (item.title || '') + '</strong>' +
            (item.year ? ' · ' + item.year : '') +
            (item.subtitle ? '<br><span class="la-text-dim">' + item.subtitle + '</span>' : '') +
            '</span>'
          row.addEventListener('mousedown', (event) => {
            event.preventDefault()
          })
          row.addEventListener('click', (event) => {
            event.preventDefault()
            addChip(item.id, { title: item.title || '', subtitle: item.subtitle || '' })
            input.value = ''
            sugg.hidden = true
            input.focus()
          })
          sugg.appendChild(row)
        })
        sugg.hidden = false
      }

      let timer = null
      input.addEventListener('input', () => {
        clearTimeout(timer)
        const q = input.value.trim()
        if (!q) {
          sugg.hidden = true
          return
        }
        timer = setTimeout(() => {
          fetch('/admin/api/cars/search?q=' + encodeURIComponent(q) + '&limit=10')
            .then((r) => (r.ok ? r.json() : { results: [] }))
            .then((data) => showSuggestions(data.results || []))
            .catch(() => { sugg.hidden = true })
        }, 180)
      })
      input.addEventListener('blur', () => {
        setTimeout(() => { sugg.hidden = true }, 200)
      })

      // Quick-add buttons (e.g. "from favorites" in the compose form).
      root.querySelectorAll('[data-cars-picker-quickadd]').forEach((btn) => {
        if (btn.dataset.bound === '1') return
        btn.dataset.bound = '1'
        btn.addEventListener('click', (event) => {
          event.preventDefault()
          addChip(btn.dataset.carId, {
            title: btn.dataset.carTitle || '',
            subtitle: btn.dataset.carSubtitle || '',
          })
        })
      })
      // The favorites quick-add buttons in the compose form sit inside
      // the field hint *outside* the picker root, so re-bind them by id.
      document.querySelectorAll('[data-cars-picker-quickadd]').forEach((btn) => {
        if (btn.dataset.bound === '1') return
        btn.dataset.bound = '1'
        btn.addEventListener('click', (event) => {
          event.preventDefault()
          addChip(btn.dataset.carId, {
            title: btn.dataset.carTitle || '',
            subtitle: btn.dataset.carSubtitle || '',
          })
        })
      })
    })
  }

  document.addEventListener('DOMContentLoaded', () => {
    initSidebar()
    initFlashFromQuery()
    initConfirmForms()
    initActiveNav()
    activateTabFromHash()
    initCarsPickers()
  })

  window.addEventListener('hashchange', () => {
    activateTabFromHash()
    document.querySelectorAll('.la-admin-nav__item.is-active').forEach((el) => {
      el.classList.remove('is-active')
    })
    initActiveNav()
  })

  window.adminToast = showToast
})()
