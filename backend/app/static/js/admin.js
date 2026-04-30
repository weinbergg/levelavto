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
    document.querySelectorAll('.la-admin-nav__item').forEach((link) => {
      const target = link.getAttribute('href')
      if (!target) return
      if (target === '/admin' && path === '/admin') {
        link.classList.add('is-active')
        return
      }
      if (target !== '/admin' && path.startsWith(target)) {
        link.classList.add('is-active')
      }
    })
  }

  document.addEventListener('DOMContentLoaded', () => {
    initSidebar()
    initFlashFromQuery()
    initConfirmForms()
    initActiveNav()
  })

  window.adminToast = showToast
})()
