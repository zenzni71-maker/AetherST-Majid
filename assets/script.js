(() => {
    'use strict';
    
    // Cache DOM elements
    const html = document.documentElement;
    const themeToggle = document.getElementById('theme-toggle');
    const mobileThemeToggle = document.getElementById('mobile-theme-toggle');
    const mobileMenuBtn = document.getElementById('mobile-menu-btn');
    const mobileMenu = document.getElementById('mobile-menu');
    const mobileOverlay = document.getElementById('mobile-overlay');
    const mobilePanel = document.getElementById('mobile-panel');
    const navbar = document.getElementById('navbar');
    const hamburgerLines = mobileMenuBtn.querySelectorAll('.hamburger-line');
    
    // State with reduced reflows
    let menuOpen = false;
    let menuAnimating = false;
    let activeTabCache = null;

    // Optimized theme detection with single media query listener
    const themeQuery = window.matchMedia('(prefers-color-scheme: dark)');
    
    function getPreferredTheme() {
        const stored = localStorage.getItem('theme');
        if (stored) return stored;
        return themeQuery.matches ? 'dark' : 'light';
    }

    // Batched DOM updates for theme changes
    function applyTheme(theme) {
        const isDark = theme === 'dark';
        html.classList.toggle('dark', isDark);
        html.classList.toggle('light', !isDark);
        localStorage.setItem('theme', theme);
    }

    applyTheme(getPreferredTheme());

    // Debounced theme toggle
    let themeToggleTimeout = null;
    function toggleTheme() {
        if (themeToggleTimeout) return;
        const current = html.classList.contains('dark') ? 'dark' : 'light';
        applyTheme(current === 'dark' ? 'light' : 'dark');
        themeToggleTimeout = setTimeout(() => { themeToggleTimeout = null; }, 50);
    }

    themeToggle.addEventListener('click', toggleTheme, { passive: true });
    mobileThemeToggle.addEventListener('click', toggleTheme, { passive: true });

    // Optimized menu toggle with batched class updates
    function openMenu() {
        if (menuAnimating) return;
        menuAnimating = true;
        menuOpen = true;

        // Batch class updates to reduce reflows
        mobileOverlay.className = 'absolute inset-0 bg-black/40 dark:bg-black/60 transition-all duration-300';
        mobilePanel.classList.remove('translate-x-full');
        mobilePanel.classList.add('translate-x-0');
        document.body.style.overflow = 'hidden';

        // Transform hamburger icon
        hamburgerLines[0].style.transform = 'rotate(45deg) translate(0, 0)';
        hamburgerLines[1].style.transform = 'scaleX(0)';
        hamburgerLines[2].style.transform = 'rotate(-45deg) translate(0, 0)';

        setTimeout(() => { menuAnimating = false; }, 300);
    }

    function closeMenu() {
        if (menuAnimating) return;
        menuAnimating = true;
        menuOpen = false;

        // Batch class updates
        mobileOverlay.className = 'absolute inset-0 bg-black/0 dark:bg-black/0 transition-all duration-300';
        mobilePanel.classList.add('translate-x-full');
        mobilePanel.classList.remove('translate-x-0');
        document.body.style.overflow = '';

        // Reset hamburger icon
        hamburgerLines[0].style.transform = '';
        hamburgerLines[1].style.transform = '';
        hamburgerLines[2].style.transform = '';

        setTimeout(() => { menuAnimating = false; }, 300);
    }

    mobileMenuBtn.addEventListener('click', () => {
        if (menuOpen) {
            closeMenu();
        } else {
            openMenu();
        }
    }, { passive: true });

    mobileOverlay.addEventListener('click', closeMenu, { passive: true });

    // Cache tab-related elements for performance
    const tabPages = document.querySelectorAll('.tab-page');
    const navTabs = document.querySelectorAll('.nav-tab');
    
    // Optimized tab switching with element caching and reduced DOM queries
    function switchTab(tabId) {
        if (activeTabCache === tabId) return; // Early exit if already active
        activeTabCache = tabId;
        
        // Use requestAnimationFrame for batched DOM updates
        requestAnimationFrame(() => {
            // Hide all pages
            tabPages.forEach(page => {
                page.classList.add('hidden');
                page.classList.remove('active');
            });

            // Show target page
            const target = document.getElementById('page-' + tabId);
            if (target) {
                target.classList.remove('hidden');
                target.classList.add('active');
            }

            // Update nav tabs - use classList.toggle for better performance
            navTabs.forEach(btn => {
                const isActive = btn.dataset.tab === tabId;
                btn.classList.toggle('bg-gray-100', isActive);
                btn.classList.toggle('dark:bg-gray-800', isActive);
                btn.classList.toggle('text-gray-900', isActive);
                btn.classList.toggle('dark:text-white', isActive);
                btn.classList.toggle('text-gray-600', !isActive);
                btn.classList.toggle('dark:text-gray-400', !isActive);
            });

            window.scrollTo({ top: 0, behavior: 'smooth' });
            localStorage.setItem('activeTab', tabId);
        });
    }

    // Event delegation for mobile menu tab buttons
    mobileMenu.addEventListener('click', (e) => {
        const tabBtn = e.target.closest('[data-tab]');
        if (tabBtn) {
            closeMenu();
            switchTab(tabBtn.dataset.tab);
        }
        const link = e.target.closest('a[href]');
        if (link) {
            closeMenu();
        }
    }, { passive: true });

    // Event delegation for desktop nav tabs
    document.addEventListener('click', (e) => {
        const tabBtn = e.target.closest('[data-tab]');
        if (tabBtn && !mobileMenu.contains(tabBtn)) {
            switchTab(tabBtn.dataset.tab);
        }
    }, { passive: true });

    // Initialize active tab
    const savedTab = localStorage.getItem('activeTab');
    if (savedTab && document.getElementById('page-' + savedTab)) {
        switchTab(savedTab);
    } else {
        switchTab('home');
    }

    // Optimized scroll handler with throttling and reduced DOM reads
    let lastScroll = 0;
    let scrollTicking = false;
    
    window.addEventListener('scroll', () => {
        if (!scrollTicking) {
            requestAnimationFrame(() => {
                const scrollY = window.scrollY;
                const isScrolled = scrollY > 10;
                
                // Only update if state changed
                if (isScrolled !== navbar.classList.contains('navbar-scrolled')) {
                    navbar.classList.toggle('navbar-scrolled', isScrolled);
                }
                
                lastScroll = scrollY;
                scrollTicking = false;
            });
            scrollTicking = true;
        }
    }, { passive: true });

    // Event delegation for config toggles
    document.addEventListener('click', (e) => {
        const toggle = e.target.closest('.config-toggle');
        if (!toggle) return;
        
        const section = toggle.closest('.config-section');
        const content = section.querySelector('.config-content');
        const icon = toggle.querySelector('svg');
        const isHidden = content.classList.contains('hidden');

        if (isHidden) {
            content.classList.remove('hidden');
            icon.classList.add('rotated');
            toggle.setAttribute('aria-expanded', 'true');
        } else {
            content.classList.add('hidden');
            icon.classList.remove('rotated');
            toggle.setAttribute('aria-expanded', 'false');
        }
    }, { passive: true });
})();