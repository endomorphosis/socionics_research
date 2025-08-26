// Modern HTML5/CSS Presentation Controller
class PresentationController {
    constructor() {
        this.currentSlide = 1;
        this.totalSlides = document.querySelectorAll('.slide').length;
        this.isFullscreen = false;
        this.searchResults = [];
        this.isOverview = false;
        
        this.init();
    }
    
    init() {
        this.bindEvents();
        this.showSlide(1);
        this.updateProgress();
        this.setupAccessibility();
        
        // Auto-hide navigation on mouse inactivity
        this.setupAutoHide();
    }
    
    bindEvents() {
        // Navigation buttons
        document.getElementById('prev-btn').addEventListener('click', () => this.previousSlide());
        document.getElementById('next-btn').addEventListener('click', () => this.nextSlide());
        document.getElementById('fullscreen-btn').addEventListener('click', () => this.toggleFullscreen());
        
        // Search functionality
        const searchInput = document.getElementById('search-input');
        searchInput.addEventListener('input', (e) => this.search(e.target.value));
        searchInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && this.searchResults.length > 0) {
                this.goToSlide(this.searchResults[0]);
            }
        });
        
        // Keyboard navigation
        document.addEventListener('keydown', (e) => this.handleKeyboard(e));
        
        // Touch/swipe support
        this.setupTouchControls();
        
        // Overview toggle (O key or double-click header)
        document.querySelector('.presentation-header').addEventListener('dblclick', () => this.toggleOverview());
        
        // Fullscreen change event
        document.addEventListener('fullscreenchange', () => this.handleFullscreenChange());
    }
    
    setupAccessibility() {
        // ARIA labels
        const slides = document.querySelectorAll('.slide');
        slides.forEach((slide, index) => {
            slide.setAttribute('aria-label', `Slide ${index + 1} of ${this.totalSlides}`);
            slide.setAttribute('role', 'region');
        });
        
        // Focus management
        document.addEventListener('focusin', (e) => {
            if (e.target.closest('.slide')) {
                this.announceSlide();
            }
        });
    }
    
    setupAutoHide() {
        let hideTimer;
        const controls = document.querySelector('.presentation-controls');
        
        const resetTimer = () => {
            clearTimeout(hideTimer);
            controls.style.opacity = '1';
            hideTimer = setTimeout(() => {
                if (!controls.matches(':hover') && !this.isOverview) {
                    controls.style.opacity = '0.7';
                }
            }, 3000);
        };
        
        document.addEventListener('mousemove', resetTimer);
        document.addEventListener('keydown', resetTimer);
        resetTimer();
    }
    
    setupTouchControls() {
        let startX = null;
        let startY = null;
        
        const slidesContainer = document.getElementById('slides-container');
        
        slidesContainer.addEventListener('touchstart', (e) => {
            startX = e.touches[0].clientX;
            startY = e.touches[0].clientY;
        }, { passive: true });
        
        slidesContainer.addEventListener('touchend', (e) => {
            if (!startX || !startY) return;
            
            const endX = e.changedTouches[0].clientX;
            const endY = e.changedTouches[0].clientY;
            
            const deltaX = startX - endX;
            const deltaY = startY - endY;
            
            // Horizontal swipe
            if (Math.abs(deltaX) > Math.abs(deltaY) && Math.abs(deltaX) > 50) {
                if (deltaX > 0) {
                    this.nextSlide(); // Swipe left -> next slide
                } else {
                    this.previousSlide(); // Swipe right -> previous slide
                }
            }
            
            startX = null;
            startY = null;
        }, { passive: true });
    }
    
    handleKeyboard(e) {
        // Prevent default if we handle the key
        const handled = true;
        
        switch(e.key) {
            case 'ArrowLeft':
            case 'ArrowUp':
            case 'PageUp':
                this.previousSlide();
                break;
            case 'ArrowRight':
            case 'ArrowDown':
            case 'PageDown':
            case ' ': // Space
                this.nextSlide();
                break;
            case 'Home':
                this.goToSlide(1);
                break;
            case 'End':
                this.goToSlide(this.totalSlides);
                break;
            case 'f':
            case 'F':
                this.toggleFullscreen();
                break;
            case 'o':
            case 'O':
                this.toggleOverview();
                break;
            case 'Escape':
                if (this.isFullscreen) this.toggleFullscreen();
                if (this.isOverview) this.toggleOverview();
                break;
            case '/':
                document.getElementById('search-input').focus();
                e.preventDefault();
                break;
            default:
                return; // Don't prevent default for unhandled keys
        }
        
        if (handled) {
            e.preventDefault();
        }
    }
    
    showSlide(slideNumber) {
        if (slideNumber < 1 || slideNumber > this.totalSlides) return;
        
        // Hide all slides
        document.querySelectorAll('.slide').forEach(slide => {
            slide.classList.remove('active');
        });
        
        // Show target slide
        const targetSlide = document.getElementById(`slide-${slideNumber}`);
        if (targetSlide) {
            targetSlide.classList.add('active');
            this.currentSlide = slideNumber;
            this.updateProgress();
            this.updateCounter();
            this.announceSlide();
        }
    }
    
    nextSlide() {
        if (this.currentSlide < this.totalSlides) {
            this.showSlide(this.currentSlide + 1);
        }
    }
    
    previousSlide() {
        if (this.currentSlide > 1) {
            this.showSlide(this.currentSlide - 1);
        }
    }
    
    goToSlide(slideNumber) {
        this.showSlide(slideNumber);
        if (this.isOverview) {
            this.toggleOverview();
        }
    }
    
    updateProgress() {
        const progress = (this.currentSlide / this.totalSlides) * 100;
        const progressFill = document.getElementById('progress-fill');
        if (progressFill) {
            progressFill.style.width = `${progress}%`;
        }
    }
    
    updateCounter() {
        const counter = document.getElementById('slide-counter');
        if (counter) {
            counter.textContent = `${this.currentSlide} / ${this.totalSlides}`;
        }
    }
    
    toggleFullscreen() {
        const container = document.querySelector('.presentation-container');
        
        if (!this.isFullscreen) {
            // Enter fullscreen
            if (container.requestFullscreen) {
                container.requestFullscreen();
            } else if (container.webkitRequestFullscreen) {
                container.webkitRequestFullscreen();
            } else if (container.msRequestFullscreen) {
                container.msRequestFullscreen();
            }
            container.classList.add('fullscreen');
            this.isFullscreen = true;
        } else {
            // Exit fullscreen
            if (document.exitFullscreen) {
                document.exitFullscreen();
            } else if (document.webkitExitFullscreen) {
                document.webkitExitFullscreen();
            } else if (document.msExitFullscreen) {
                document.msExitFullscreen();
            }
            container.classList.remove('fullscreen');
            this.isFullscreen = false;
        }
    }
    
    handleFullscreenChange() {
        const container = document.querySelector('.presentation-container');
        if (!document.fullscreenElement) {
            container.classList.remove('fullscreen');
            this.isFullscreen = false;
        }
    }
    
    toggleOverview() {
        const overview = document.getElementById('slide-overview');
        this.isOverview = !this.isOverview;
        
        if (this.isOverview) {
            overview.style.display = 'block';
            this.announceOverview();
        } else {
            overview.style.display = 'none';
        }
    }
    
    search(query) {
        this.searchResults = [];
        
        if (!query.trim()) {
            this.clearHighlights();
            return;
        }
        
        const slides = document.querySelectorAll('.slide');
        const searchRegex = new RegExp(query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi');
        
        slides.forEach((slide, index) => {
            const textContent = slide.textContent;
            const slideNumber = index + 1;
            
            if (searchRegex.test(textContent)) {
                this.searchResults.push(slideNumber);
            }
        });
        
        this.highlightSearchResults(query);
        
        // Announce search results
        this.announceSearchResults(query);
    }
    
    highlightSearchResults(query) {
        this.clearHighlights();
        
        if (!query.trim()) return;
        
        const slides = document.querySelectorAll('.slide');
        const searchRegex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
        
        slides.forEach(slide => {
            this.highlightInElement(slide, searchRegex);
        });
    }
    
    highlightInElement(element, regex) {
        const walker = document.createTreeWalker(
            element,
            NodeFilter.SHOW_TEXT,
            null,
            false
        );
        
        const textNodes = [];
        let node;
        while (node = walker.nextNode()) {
            textNodes.push(node);
        }
        
        textNodes.forEach(textNode => {
            const text = textNode.textContent;
            if (regex.test(text)) {
                const highlightedHTML = text.replace(regex, '<mark class="search-highlight">$1</mark>');
                const wrapper = document.createElement('span');
                wrapper.innerHTML = highlightedHTML;
                textNode.parentNode.replaceChild(wrapper, textNode);
            }
        });
    }
    
    clearHighlights() {
        document.querySelectorAll('.search-highlight').forEach(mark => {
            const parent = mark.parentNode;
            parent.replaceChild(document.createTextNode(mark.textContent), mark);
            parent.normalize();
        });
    }
    
    announceSlide() {
        const currentSlideElement = document.querySelector('.slide.active');
        if (currentSlideElement) {
            const title = currentSlideElement.querySelector('.slide-title');
            const titleText = title ? title.textContent : `Slide ${this.currentSlide}`;
            
            // Create announcement for screen readers
            this.announce(`${titleText}. Slide ${this.currentSlide} of ${this.totalSlides}.`);
        }
    }
    
    announceOverview() {
        this.announce(`Slide overview mode. ${this.totalSlides} slides available. Use arrow keys to navigate, Enter to select, or Escape to close.`);
    }
    
    announceSearchResults(query) {
        const count = this.searchResults.length;
        if (count > 0) {
            this.announce(`Found ${count} slide${count === 1 ? '' : 's'} matching "${query}". Press Enter to go to first result.`);
        } else {
            this.announce(`No slides found matching "${query}".`);
        }
    }
    
    announce(message) {
        // Create or update live region for screen readers
        let liveRegion = document.getElementById('live-announcements');
        if (!liveRegion) {
            liveRegion = document.createElement('div');
            liveRegion.id = 'live-announcements';
            liveRegion.setAttribute('aria-live', 'polite');
            liveRegion.setAttribute('aria-atomic', 'true');
            liveRegion.className = 'sr-only';
            document.body.appendChild(liveRegion);
        }
        
        liveRegion.textContent = message;
    }
    
    // Timer functionality
    startTimer() {
        if (this.timerInterval) return;
        
        this.timerStart = Date.now();
        this.timerInterval = setInterval(() => {
            this.updateTimer();
        }, 1000);
    }
    
    stopTimer() {
        if (this.timerInterval) {
            clearInterval(this.timerInterval);
            this.timerInterval = null;
        }
    }
    
    updateTimer() {
        const elapsed = Math.floor((Date.now() - this.timerStart) / 1000);
        const minutes = Math.floor(elapsed / 60);
        const seconds = elapsed % 60;
        
        // Update timer display if it exists
        const timerDisplay = document.getElementById('timer-display');
        if (timerDisplay) {
            timerDisplay.textContent = `${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
        }
    }
}

// Global functions for overview navigation
window.goToSlide = function(slideNumber) {
    if (window.presentationController) {
        window.presentationController.goToSlide(slideNumber);
    }
};

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Add search result highlighting styles
    const style = document.createElement('style');
    style.textContent = `
        .search-highlight {
            background-color: #ffeb3b;
            color: #000;
            padding: 0 2px;
            border-radius: 2px;
        }
        
        @media print {
            .search-highlight {
                background-color: transparent;
                border: 1px solid #000;
            }
        }
    `;
    document.head.appendChild(style);
    
    // Initialize presentation controller
    window.presentationController = new PresentationController();
    
    // Add help dialog if needed
    if (!document.getElementById('help-dialog')) {
        const helpDialog = document.createElement('div');
        helpDialog.id = 'help-dialog';
        helpDialog.innerHTML = `
            <div class="help-content">
                <h3>Keyboard Shortcuts</h3>
                <ul>
                    <li><kbd>→</kbd> <kbd>↓</kbd> <kbd>Space</kbd> <kbd>PgDn</kbd> - Next slide</li>
                    <li><kbd>←</kbd> <kbd>↑</kbd> <kbd>PgUp</kbd> - Previous slide</li>
                    <li><kbd>Home</kbd> - First slide</li>
                    <li><kbd>End</kbd> - Last slide</li>
                    <li><kbd>F</kbd> - Toggle fullscreen</li>
                    <li><kbd>O</kbd> - Toggle overview</li>
                    <li><kbd>/</kbd> - Focus search</li>
                    <li><kbd>Esc</kbd> - Exit fullscreen/overview</li>
                </ul>
                <p>Touch: Swipe left/right to navigate slides</p>
            </div>
        `;
        document.body.appendChild(helpDialog);
    }
    
    console.log('Enhanced HTML5/CSS Presentation System Loaded');
    console.log('Keyboard shortcuts: Arrow keys, Space, F (fullscreen), O (overview), / (search)');
});