const API_BASE = "http://localhost:8000/api/v1";
let activeChartInstances = {};

document.addEventListener("DOMContentLoaded", () => {
    initNavigation();
    initChatForm();
    initPromptChips();
    checkUrlParams();
    initResetContextBtn();
    fetchWarehouseHealth();
    fetchExecutiveKPIs();
});

/* Navigation Tabs */
function initNavigation() {
    const navBtns = document.querySelectorAll(".nav-btn");
    navBtns.forEach(btn => {
        btn.addEventListener("click", () => {
            navBtns.forEach(b => b.classList.remove("active"));
            document.querySelectorAll(".tab-page").forEach(p => p.classList.remove("active"));

            btn.classList.add("active");
            const tabId = `tab-${btn.dataset.tab}`;
            const targetPage = document.getElementById(tabId);
            if (targetPage) targetPage.classList.add("active");

            if (btn.dataset.tab === "kpis") fetchExecutiveKPIs();
            if (btn.dataset.tab === "schema") fetchWarehouseSchema();
        });
    });
}

/* Check Health */
async function fetchWarehouseHealth() {
    try {
        const res = await fetch(`${API_BASE}/health`);
        const data = await res.json();
        const indicator = document.querySelector(".status-indicator");
        const subtitle = document.getElementById("dw-name");

        if (data.status === "healthy") {
            indicator.className = "status-indicator online";
            subtitle.innerText = `${data.target_dw} (PostgreSQL Gold)`;
        } else {
            indicator.className = "status-indicator offline";
            subtitle.innerText = "Connection Degraded";
        }
    } catch (e) {
        console.warn("Backend health check error:", e);
    }
}

/* Chat Form & API Integration */
function initChatForm() {
    const form = document.getElementById("chat-form");
    const input = document.getElementById("user-input");
    const clearBtn = document.getElementById("btn-clear-chat");

    form.addEventListener("submit", (e) => {
        e.preventDefault();
        const query = input.value.trim();
        if (!query) return;

        sendChatQuery(query);
        input.value = "";
    });

    if (clearBtn) {
        clearBtn.addEventListener("click", () => {
            const container = document.getElementById("chat-messages");
            container.innerHTML = `
                <div class="message system-msg">
                    <div class="msg-avatar">N</div>
                    <div class="msg-content">Chat history cleared. Ready for your analysis questions.</div>
                </div>
            `;
        });
    }
}

function initPromptChips() {
    document.addEventListener("click", (e) => {
        if (e.target.classList.contains("chip")) {
            const prompt = e.target.dataset.prompt;
            if (prompt) sendChatQuery(prompt);
        }
    });
}

/* Read URL Query Parameters for Power BI Deep Links */
function checkUrlParams() {
    const params = new URLSearchParams(window.location.search);
    const query = params.get("query") || params.get("q");
    const country = params.get("country");
    const year = params.get("year");
    const type = params.get("type");

    let contextDesc = [];
    if (country) contextDesc.push(`Country: ${country}`);
    if (year) contextDesc.push(`Year: ${year}`);
    if (type) contextDesc.push(`Type: ${type}`);

    if (contextDesc.length > 0) {
        const card = document.getElementById("pbi-context-card");
        const text = document.getElementById("pbi-context-text");
        if (card && text) {
            text.innerText = contextDesc.join(" | ");
            card.style.display = "flex";
        }
    }

    if (query) {
        let finalQuery = query;
        if (country && !query.toLowerCase().includes(country.toLowerCase())) {
            finalQuery += ` in ${country}`;
        }
        setTimeout(() => sendChatQuery(finalQuery), 400);
    }
}

function initResetContextBtn() {
    const resetBtn = document.getElementById("btn-reset-context");
    if (resetBtn) {
        resetBtn.addEventListener("click", () => {
            window.history.pushState({}, document.title, window.location.pathname);
            const card = document.getElementById("pbi-context-card");
            if (card) card.style.display = "none";
        });
    }
}

/* Send Chat Prompt to Backend Orchestrator */
async function sendChatQuery(query) {
    const messagesContainer = document.getElementById("chat-messages");

    // 1. Append User Message
    appendMessage("user", query);

    // 2. Append Loading AI Message Placeholder
    const loadingId = `msg-${Date.now()}`;
    appendLoadingMessage(loadingId);
    scrollToBottom();

    // Build context payload if URL params exist
    const params = new URLSearchParams(window.location.search);
    let contextPayload = null;
    if (params.get("country") || params.get("year") || params.get("type")) {
        contextPayload = {
            country: params.get("country") || undefined,
            year: params.get("year") ? parseInt(params.get("year")) : undefined,
            type: params.get("type") || undefined
        };
    }

    try {
        const response = await fetch(`${API_BASE}/chat`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query: query, context: contextPayload })
        });

        const data = await response.json();
        removeMessage(loadingId);

        if (response.ok) {
            appendAIMessage(data);
        } else {
            appendErrorMessage(data.detail || "Unable to complete request. Please try again.");
        }
    } catch (err) {
        removeMessage(loadingId);
        appendErrorMessage(`Network Error: Cannot connect to Netflix AI API. Verify backend server is running.`);
    }

    scrollToBottom();
}

/* Message Render Helpers */
function appendMessage(role, text) {
    const container = document.getElementById("chat-messages");
    const msgDiv = document.createElement("div");
    msgDiv.className = `message ${role}-msg`;
    msgDiv.innerHTML = `
        <div class="msg-avatar">${role === 'user' ? 'YOU' : 'N'}</div>
        <div class="msg-content">${escapeHtml(text)}</div>
    `;
    container.appendChild(msgDiv);
}

function appendLoadingMessage(id) {
    const container = document.getElementById("chat-messages");
    const msgDiv = document.createElement("div");
    msgDiv.id = id;
    msgDiv.className = "message ai-msg";
    msgDiv.innerHTML = `
        <div class="msg-avatar">N</div>
        <div class="msg-content">
            <div style="display: flex; align-items: center; gap: 0.75rem; color: var(--text-muted);">
                <div class="spinner"></div>
                <span>Netflix AI is analyzing your data...</span>
            </div>
        </div>
    `;
    container.appendChild(msgDiv);
}

function removeMessage(id) {
    const el = document.getElementById(id);
    if (el) el.remove();
}

function appendErrorMessage(errorText) {
    const container = document.getElementById("chat-messages");
    const msgDiv = document.createElement("div");
    msgDiv.className = "message ai-msg";
    msgDiv.innerHTML = `
        <div class="msg-avatar">N</div>
        <div class="msg-content" style="border-color: rgba(229, 9, 20, 0.4); background: rgba(229, 9, 20, 0.08);">
            <strong style="color: var(--accent-red);">System Notice:</strong>
            <p style="margin-top: 0.3rem;">${escapeHtml(errorText)}</p>
        </div>
    `;
    container.appendChild(msgDiv);
}

function appendAIMessage(data) {
    const container = document.getElementById("chat-messages");
    const msgDiv = document.createElement("div");
    msgDiv.className = "message ai-msg";

    const chartCanvasId = `chart-${Date.now()}`;

    // 1. Meta Badges (Intent & Agents Used)
    let metaBadgesHtml = "";
    if (data.intent || (data.agents_used && data.agents_used.length > 0)) {
        const intentHtml = data.intent ? `<span class="intent-badge">🎯 Intent: ${escapeHtml(data.intent)}</span>` : "";
        const agentTags = (data.agents_used || []).map(a => `<span class="agent-tag">🤖 ${escapeHtml(a)}</span>`).join("");
        metaBadgesHtml = `<div class="meta-badges">${intentHtml}${agentTags}</div>`;
    }

    // 2. Polished Trajectory Steps (`● Agent — Status`)
    let trajectoryHtml = "";
    if (data.trajectory && data.trajectory.length > 0) {
        const stepsItems = data.trajectory.map(s => {
            const statusClass = (s.status || "Completed").toLowerCase();
            return `
                <div class="step-item">
                    <span class="step-dot" style="color: ${statusClass === 'completed' ? '#4ade80' : '#38bdf8'};">●</span>
                    <strong>${escapeHtml(s.agent)}</strong> — 
                    <span class="step-badge ${statusClass}">${escapeHtml(s.status)}</span>
                </div>
            `;
        }).join("");

        trajectoryHtml = `
            <div class="agent-trajectory">
                <div class="trajectory-header">
                    <span>🧠 Multi-Agent Execution Trajectory (${data.execution_time_ms}ms)</span>
                </div>
                <div class="trajectory-steps">${stepsItems}</div>
            </div>
        `;
    }

    // 3. Insights Bullets Card
    let insightsHtml = "";
    if (data.insights && data.insights.length > 0) {
        const bullets = data.insights.map(i => `<li>${escapeHtml(i)}</li>`).join("");
        insightsHtml = `
            <div class="insights-card">
                <div class="insights-card-title">💡 Insight Findings</div>
                <ul class="insights-list">${bullets}</ul>
            </div>
        `;
    }

    // 4. SQL Code Block
    let sqlHtml = "";
    if (data.sql_query) {
        sqlHtml = `<div class="sql-box">-- Read-Only SQL Query Executed against PostgreSQL Gold DW\n${escapeHtml(data.sql_query)}</div>`;
    }

    // 5. Chart Canvas HTML
    let chartHtml = "";
    if (data.chart && data.chart.data && data.chart.data.length > 0) {
        chartHtml = `
            <div class="chart-card">
                <canvas id="${chartCanvasId}"></canvas>
            </div>
        `;
    }

    msgDiv.innerHTML = `
        <div class="msg-avatar">N</div>
        <div class="msg-content">
            ${metaBadgesHtml}
            ${trajectoryHtml}
            <div>${formatMarkdown(data.answer)}</div>
            ${insightsHtml}
            ${sqlHtml}
            ${chartHtml}
        </div>
    `;

    container.appendChild(msgDiv);

    // Render Chart if spec exists
    if (data.chart && data.chart.data && data.chart.data.length > 0) {
        setTimeout(() => renderChart(chartCanvasId, data.chart), 100);
    }
}

/* Dynamic Chart.js Renderer */
function renderChart(canvasId, chartSpec) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    const labels = chartSpec.data.map(d => d[chartSpec.x_key]);
    const yKey = chartSpec.y_keys[0];
    const datasetValues = chartSpec.data.map(d => d[yKey]);

    let chartType = chartSpec.chart_type;
    let indexAxis = 'x';
    if (chartType === 'horizontal_bar') {
        chartType = 'bar';
        indexAxis = 'y';
    } else if (chartType === 'donut') {
        chartType = 'doughnut';
    }

    const colors = [
        '#E50914', '#38bdf8', '#4ade80', '#a855f7', '#f59e0b',
        '#ec4899', '#6366f1', '#14b8a6', '#f97316', '#84cc16'
    ];

    activeChartInstances[canvasId] = new Chart(ctx, {
        type: chartType,
        data: {
            labels: labels,
            datasets: [{
                label: chartSpec.title || yKey,
                data: datasetValues,
                backgroundColor: chartType === 'line' ? 'rgba(229, 9, 20, 0.25)' : colors.slice(0, labels.length),
                borderColor: chartType === 'line' ? '#E50914' : 'transparent',
                borderWidth: chartType === 'line' ? 3 : 0,
                fill: chartType === 'line',
                tension: 0.3
            }]
        },
        options: {
            indexAxis: indexAxis,
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: chartSpec.title,
                    color: '#f3f4f6',
                    font: { family: 'Outfit', size: 14, weight: 'bold' }
                },
                legend: {
                    labels: { color: '#9ca3af', font: { family: 'Inter' } }
                }
            },
            scales: chartType === 'doughnut' ? {} : {
                x: {
                    ticks: { color: '#9ca3af', font: { family: 'Inter', size: 11 } },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                },
                y: {
                    ticks: { color: '#9ca3af', font: { family: 'Inter', size: 11 } },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                }
            }
        }
    });
}

/* Fetch & Render Live Executive KPIs */
async function fetchExecutiveKPIs() {
    const grid = document.getElementById("kpi-cards-grid");
    try {
        const res = await fetch(`${API_BASE}/kpis`);
        const data = await res.json();

        grid.innerHTML = `
            <div class="kpi-metric-card">
                <span class="kpi-metric-title">Total Catalog Titles</span>
                <span class="kpi-metric-value">${data.total_titles.toLocaleString()}</span>
                <span class="kpi-metric-subtitle">Distinct Show IDs in Warehouse</span>
            </div>
            <div class="kpi-metric-card">
                <span class="kpi-metric-title">Movie Ratio</span>
                <span class="kpi-metric-value">${data.movie_ratio_pct}%</span>
                <span class="kpi-metric-subtitle">${data.total_movies.toLocaleString()} Movies</span>
            </div>
            <div class="kpi-metric-card">
                <span class="kpi-metric-title">TV Show Ratio</span>
                <span class="kpi-metric-value">${data.tv_ratio_pct}%</span>
                <span class="kpi-metric-subtitle">${data.total_tv_shows.toLocaleString()} TV Shows</span>
            </div>
            <div class="kpi-metric-card">
                <span class="kpi-metric-title">Average Content Age</span>
                <span class="kpi-metric-value">${data.average_content_age} yrs</span>
                <span class="kpi-metric-subtitle">From Release Year to Present</span>
            </div>
            <div class="kpi-metric-card">
                <span class="kpi-metric-title">Top Producing Country</span>
                <span class="kpi-metric-value" style="font-size: 1.6rem;">${data.top_country}</span>
                <span class="kpi-metric-subtitle">Leading Content Origin</span>
            </div>
            <div class="kpi-metric-card">
                <span class="kpi-metric-title">Top Genre Category</span>
                <span class="kpi-metric-value" style="font-size: 1.4rem;">${data.top_genre}</span>
                <span class="kpi-metric-subtitle">Highest Volume Category</span>
            </div>
        `;
    } catch (e) {
        grid.innerHTML = `<div style="color: var(--accent-red);">Error loading KPIs: ${e.message}</div>`;
    }
}

/* Fetch & Render Gold Schema */
async function fetchWarehouseSchema() {
    const container = document.getElementById("schema-content");
    try {
        const res = await fetch(`${API_BASE}/schema`);
        const data = await res.json();

        let html = ``;
        for (const [tableName, cols] of Object.entries(data)) {
            html += `
                <div style="margin-bottom: 2rem;">
                    <h3 style="color: var(--accent-blue); font-size: 1.05rem; font-family: var(--font-heading);">📍 ${tableName}</h3>
                    <table class="schema-table">
                        <thead>
                            <tr><th>Column Name</th><th>Data Type</th></tr>
                        </thead>
                        <tbody>
                            ${cols.map(c => `<tr><td><code>${c.column}</code></td><td>${c.type}</td></tr>`).join("")}
                        </tbody>
                    </table>
                </div>
            `;
        }
        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = `<div style="color: var(--accent-red);">Error loading schema: ${e.message}</div>`;
    }
}

/* Utilities */
function scrollToBottom() {
    const container = document.getElementById("chat-messages");
    container.scrollTop = container.scrollHeight;
}

function escapeHtml(text) {
    if (!text) return "";
    return text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function formatMarkdown(text) {
    if (!text) return "";
    let html = escapeHtml(text);
    // Bold
    html = html.replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>");
    // Code
    html = html.replace(/`(.*?)`/g, "<code>$1</code>");
    // Headings
    html = html.replace(/### (.*?)\n/g, "<h3 style='margin: 0.5rem 0; color: #fff;'>$1</h3>");
    // Newlines
    html = html.replace(/\n/g, "<br>");
    return html;
}
