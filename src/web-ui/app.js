// ── EON AI CPW — Web UI ───────────────────────────────────────────
const CLOUD = 'https://eon-p2p-cloud.exportdefaultasyncfetchrequestenvconsturl.workers.dev';
const AUTH = '8762b4a71b4ae01536b6d663ea1143d0b2b30c2b893044a8bce7fab0ebecfba9';
const API = ''; // calls cloud worker directly (CORS allows all origins)
let activeAgent = 'auto';
let isProcessing = false;
let recognition = null;
let conversation = [{ role: 'system', content: 'You are EON AI CPW, the most powerful autonomous AI infrastructure in the world. You are helpful, creative, and capable. You can delegate to specialized agents, browse the web, generate images, and remember past conversations.' }];

const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);

// ── Initialize ────────────────────────────────────────────────────
async function init() {
  checkStatus();
  setInterval(checkStatus, 15000);
  setupInput();
  setupAgents();
  setupVoice();
  setupImage();
  setupGeneration();
  setupMemory();
  setupWebAgent();
  setupSystem();
  setupKeyboard();
  addMessage('system', 'Welcome to EON AI CPW. Ask anything, or select an agent type above.');
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js').catch(() => {});
  }
}

// ── Status check ──────────────────────────────────────────────────
async function checkStatus() {
  try {
    const data = await apiGet('/status');
    const dot = $('#statusDot');
    const txt = $('#statusText');
    dot.className = 'status-dot online';
    txt.textContent = 'online';
  } catch {
    const dot = $('#statusDot');
    const txt = $('#statusText');
    dot.className = 'status-dot degraded';
    txt.textContent = 'degraded';
  }
}

// ── API helpers ───────────────────────────────────────────────────
async function apiGet(path) {
  const r = await fetch(`${CLOUD}${path}`, {
    headers: { 'Authorization': `Bearer ${AUTH}` }
  });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

async function apiPost(path, body) {
  const r = await fetch(`${CLOUD}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH}` },
    body: JSON.stringify(body),
  });
  if (!r.ok) { const e = await r.json().catch(() => ({error:r.statusText})); throw new Error(e.error||r.statusText); }
  return r.json();
}

// ── Send message ──────────────────────────────────────────────────
async function send(text) {
  if (!text.trim() || isProcessing) return;
  isProcessing = true;
  const sendBtn = $('#sendBtn');
  sendBtn.disabled = true;

  addMessage('user', text, activeAgent);
  $('#input').value = '';
  autoResize();

  // Build conversation history
  const sysPrompt = `You are EON AI CPW, the most powerful autonomous AI infrastructure. You are currently acting as a ${activeAgent} agent. Respond helpfully and naturally. You have memory, vision, voice, image generation, and 9 agent types available.`;
  const msgs = [{ role: 'system', content: sysPrompt }];
  // Take last 20 messages for context window
  const recent = conversation.slice(-20);
  for (const m of recent) {
    if (m.role !== 'system') msgs.push(m);
  }
  msgs.push({ role: 'user', content: text });
  conversation.push({ role: 'user', content: text });

  const msgEl = addMessage('assistant', '', activeAgent, true);
  const contentEl = msgEl.querySelector('.msg-content');
  const metaEl = msgEl.querySelector('.msg-meta');

  let fullResponse = '';
  let provider = '';

  try {
    const r = await fetch(`${CLOUD}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH}` },
      body: JSON.stringify({
        model: activeAgent === 'auto' ? 'llama-3.3-70b' : activeAgent,
        messages: msgs,
        max_tokens: 2000,
        stream: true,
      }),
    });

    if (!r.ok) {
      const errBody = await r.json().catch(() => ({ error: `HTTP ${r.status}` }));
      throw new Error(errBody.error || `HTTP ${r.status}`);
    }

    const reader = r.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      for (const line of lines) {
        if (line.startsWith('data: ')) {
          const data = line.slice(6).trim();
          if (data === '[DONE]') continue;
          try {
            const parsed = JSON.parse(data);
            const choice = parsed.choices?.[0];
            if (choice?.delta?.content) {
              fullResponse += choice.delta.content;
              contentEl.textContent = fullResponse;
              scrollToBottom();
            }
            if (choice?.delta?.provider) provider = choice.delta.provider;
            if (parsed.provider) provider = parsed.provider;
          } catch {}
        }
      }
    }

    if (!fullResponse) {
      fullResponse = 'No response from model';
      contentEl.textContent = fullResponse;
    }
  } catch (err) {
    fullResponse = `Error: ${err.message}`;
    contentEl.textContent = fullResponse;
  }

  msgEl.classList.remove('typing');
  if (provider) metaEl.textContent = `via ${provider}`;
  conversation.push({ role: 'assistant', content: fullResponse });
  isProcessing = false;
  sendBtn.disabled = false;
  $('#input').focus();
}

// ── Add message ───────────────────────────────────────────────────
function addMessage(role, content, agent, typing) {
  const el = document.createElement('div');
  el.className = `msg ${role}${typing ? ' typing' : ''}`;
  const avatarText = role === 'user' ? 'U' : role === 'assistant' ? 'AI' : '◆';
  const agentLabel = agent && agent !== 'auto' ? `<span class="msg-agent">${agent}</span>` : '';
  el.innerHTML = `
    <div class="msg-avatar">${avatarText}</div>
    <div class="msg-body">
      <div class="msg-name">${role === 'user' ? 'You' : role === 'assistant' ? 'EON' : 'System'}${agentLabel}</div>
      <div class="msg-content">${typing ? '' : escapeHtml(content)}</div>
      <div class="msg-meta"></div>
    </div>`;
  $('#messages').appendChild(el);
  scrollToBottom();
  return el;
}

function scrollToBottom() {
  const m = $('#messages');
  m.scrollTop = m.scrollHeight;
}

// ── Input handling ────────────────────────────────────────────────
function setupInput() {
  const input = $('#input');
  input.addEventListener('input', autoResize);
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(input.value); }
  });
  $('#sendBtn').addEventListener('click', () => send(input.value));
}

function autoResize() {
  const el = $('#input');
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 120) + 'px';
}

// ── Agent selection ───────────────────────────────────────────────
function setupAgents() {
  $$('.agent-chip').forEach(btn => {
    btn.addEventListener('click', () => {
      $$('.agent-chip').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      activeAgent = btn.dataset.agent;
      toast(`Agent: ${btn.textContent}`);
    });
  });
}

// ── Voice input ───────────────────────────────────────────────────
function setupVoice() {
  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SpeechRecognition) {
    $('#voiceBtn').style.opacity = '0.3';
    $('#voiceBtn').title = 'Voice not supported';
    return;
  }
  recognition = new SpeechRecognition();
  recognition.continuous = false;
  recognition.interimResults = true;
  recognition.lang = 'en-US';
  let isListening = false;

  recognition.onresult = e => {
    const transcript = Array.from(e.results).map(r => r[0].transcript).join('');
    $('#input').value = transcript;
    autoResize();
    if (e.results[e.results.length-1].isFinal) {
      send(transcript);
    }
  };
  recognition.onerror = () => { isListening = false; $('#voiceBtn').style.background = ''; };
  recognition.onend = () => { isListening = false; $('#voiceBtn').style.background = ''; };

  $('#voiceBtn').addEventListener('click', () => {
    if (isListening) { recognition.stop(); isListening = false; $('#voiceBtn').style.background = ''; return; }
    try {
      recognition.start();
      isListening = true;
      $('#voiceBtn').style.background = 'rgba(239,68,68,0.3)';
      toast('Listening...');
    } catch { toast('Voice error'); }
  });
}

// ── Image upload (vision) ─────────────────────────────────────────
function setupImage() {
  const input = $('#imageInput');
  $('#imageBtn').addEventListener('click', () => input.click());
  input.addEventListener('change', async () => {
    const file = input.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = async e => {
      const dataUrl = e.target.result;
      addMessage('user', `[Image: ${file.name}]`, activeAgent);
      addMessage('assistant', 'Processing image via Workers AI...', activeAgent, true);
      try {
        const r = await fetch(`${CLOUD}/v1/chat/completions`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH}` },
          body: JSON.stringify({
            model: 'llama-3.3-70b',
            messages: [{ role: 'user', content: [{ type:'text', text:'Describe this image' }, { type:'image_url', image_url:{url:dataUrl} }] }],
            max_tokens: 500,
          }),
        });
        const data = await r.json();
        const msg = $$('.msg.assistant.typing');
        if (msg.length) { msg[msg.length-1].classList.remove('typing'); msg[msg.length-1].querySelector('.msg-content').textContent = data.choices?.[0]?.message?.content || 'Could not process'; }
      } catch (err) {
        const msg = $$('.msg.assistant.typing');
        if (msg.length) { msg[msg.length-1].classList.remove('typing'); msg[msg.length-1].querySelector('.msg-content').textContent = `Vision error: ${err.message}`; }
      }
    };
    reader.readAsDataURL(file);
    input.value = '';
  });
}

// ── Image generation ──────────────────────────────────────────────
function setupGeneration() {
  $('#genBtn').addEventListener('click', async () => {
    const prompt = $('#input').value.trim();
    if (!prompt) { toast('Enter a prompt first'); return; }
    addMessage('user', `Generate: ${prompt}`, activeAgent);
    const msgEl = addMessage('assistant', 'Generating...', activeAgent, true);
    try {
      const r = await fetch(`https://text.pollinations.ai/openai/v1/images/generations`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ model: 'flux', prompt, n: 2 }),
      });
      const data = await r.json();
      msgEl.classList.remove('typing');
      const urls = data.data?.map(d => d.url).filter(Boolean) || [];
      if (urls.length) {
        msgEl.querySelector('.msg-content').innerHTML = `<div class="img-gen-grid">${urls.map(u => `<img src="${u}" loading="lazy">`).join('')}</div>`;
      } else {
        msgEl.querySelector('.msg-content').textContent = 'Generation returned no images';
      }
    } catch (err) {
      msgEl.classList.remove('typing');
      msgEl.querySelector('.msg-content').textContent = `Generation error: ${err.message}`;
    }
  });
}

// ── Dream memory ──────────────────────────────────────────────────
function setupMemory() {
  $('#memoryBtn').addEventListener('click', showMemory);
}

async function showMemory() {
  openPanel('Dream Memory');
  const pc = $('#panelContent');
  pc.innerHTML = '<div class="panel-section"><div class="panel-card"><div class="value">Loading...</div></div></div>';
  try {
    const stats = await apiGet('/dream/stats');
    const entries = await apiGet('/dream/insights');
    const upgrades = await apiGet('/upgrade/list');

    pc.innerHTML = `
      <div class="panel-section">
        <h3>Dream Memory</h3>
        <div class="panel-card">
          <div class="row"><span class="k">Entries</span><span class="v">${stats.total || 0}</span></div>
          <div class="row"><span class="k">Upgrades</span><span class="v">${upgrades.total || 0}</span></div>
        </div>
      </div>
      ${entries.entries?.length ? `
      <div class="panel-section">
        <h3>Dreams & Insights</h3>
        ${entries.entries.slice(-10).reverse().map(e => `
          <div class="panel-card">
            <div class="row"><span class="k">${e.type}</span><span class="v">p${e.priority}</span></div>
            <div style="font-size:13px;margin-top:4px">${escapeHtml(e.title)}</div>
            <div style="font-size:11px;color:var(--text-dim);margin-top:2px">${escapeHtml(e.description?.slice(0,100))}</div>
          </div>
        `).join('')}
      </div>` : ''}
      ${upgrades.upgrades?.length ? `
      <div class="panel-section">
        <h3>Upgrade Proposals</h3>
        ${upgrades.upgrades.slice(-10).reverse().map(u => `
          <div class="panel-card">
            <div class="row"><span class="k">${u.target}</span><span class="v">${u.status}</span></div>
            <div style="font-size:13px;margin-top:4px">${escapeHtml(u.title)}</div>
            <div style="font-size:11px;color:var(--text-dim);margin-top:2px">${escapeHtml(u.description?.slice(0,100))}</div>
          </div>
        `).join('')}
      </div>` : ''}
      <div class="panel-section">
        <button class="agent-chip" onclick="triggerDreamCycle()" style="width:100%;text-align:center">Trigger Dream Cycle</button>
      </div>
    `;
  } catch (err) {
    pc.innerHTML = `<div class="panel-section"><div class="panel-card"><div class="row"><span class="k">Error</span><span class="v" style="color:var(--error)">${err.message}</span></div></div></div>`;
  }
}

async function triggerDreamCycle() {
  toast('Triggering dream cycle...');
  try {
    const r = await apiPost('/dream/cycle', {});
    toast(`Dream cycle: ${r.dreams} dreams, ${r.upgrades} upgrades`);
    showMemory();
  } catch (err) { toast(`Error: ${err.message}`); }
}

// ── Web Agent ─────────────────────────────────────────────────────
function setupWebAgent() {
  $('#webAgentBtn').addEventListener('click', () => {
    $('#webAgentModal').classList.remove('hidden');
    $('#webAgentUrl').focus();
  });
  $('#webAgentClose').addEventListener('click', () => $('#webAgentModal').classList.add('hidden'));
  $('#webAgentModal .modal-bg').addEventListener('click', () => $('#webAgentModal').classList.add('hidden'));

  $('#webAgentBrowse').addEventListener('click', () => webAgentAction('fetch'));
  $('#webAgentSearch').addEventListener('click', () => webAgentAction('search'));
  $('#webAgentLLM').addEventListener('click', webAgentAnalyze);

  $('#webAgentUrl').addEventListener('keydown', e => {
    if (e.key === 'Enter') webAgentAction('fetch');
  });
}

async function webAgentAction(action) {
  const url = $('#webAgentUrl').value.trim();
  const resultEl = $('#webAgentResult');
  resultEl.classList.remove('hidden');
  resultEl.innerHTML = '<div style="color:var(--text-dim)">Browsing...</div>';

  try {
    const body = action === 'search'
      ? { url: '', action: 'search', query: url }
      : { url, action: 'fetch' };
    const r = await fetch(`${CLOUD}/web-agent`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH}` },
      body: JSON.stringify(body),
    });
    const data = await r.json();

    if (!data.success) {
      resultEl.innerHTML = `<div style="color:var(--error)">Error: ${data.error}</div>`;
      return;
    }

    const textPreview = data.text ? data.text.slice(0, 2000) : '';
    resultEl.innerHTML = `
      <div class="row"><span class="k">URL</span><span class="v" style="font-size:11px">${escapeHtml(data.url)}</span></div>
      <div class="row"><span class="k">Status</span><span class="v">${data.status}</span></div>
      <div class="row"><span class="k">Title</span><span class="v">${escapeHtml(data.title || '-')}</span></div>
      <div class="row"><span class="k">Size</span><span class="v">${(data.size / 1024).toFixed(1)}KB</span></div>
      <div class="row"><span class="k">Elapsed</span><span class="v">${data.elapsed}ms</span></div>
      ${data.links?.length ? `<h4>Links (${data.links.length})</h4>${data.links.slice(0, 10).map(l => `<a href="${escapeHtml(l.href)}" target="_blank">${escapeHtml(l.text || l.href)}</a>`).join('')}${data.links.length > 10 ? `<div style="color:var(--text-dim);font-size:11px">+${data.links.length-10} more</div>` : ''}` : ''}
      ${data.images?.length ? `<h4>Images (${data.images.length})</h4>${data.images.slice(0, 5).map(i => `<img src="${escapeHtml(i.src)}" style="max-width:100px;border-radius:4px;margin:2px">`).join('')}` : ''}
      ${textPreview ? `<h4>Content</h4><div style="font-size:12px;line-height:1.6">${escapeHtml(textPreview)}</div>` : ''}
    `;
  } catch (err) {
    resultEl.innerHTML = `<div style="color:var(--error)">Error: ${err.message}</div>`;
  }
}

async function webAgentAnalyze() {
  const url = $('#webAgentUrl').value.trim();
  if (!url) { toast('Enter a URL first'); return; }
  $('#webAgentModal').classList.add('hidden');
  addMessage('user', `Browse and analyze: ${url}`, 'researcher');
  const msgEl = addMessage('assistant', 'Fetching page...', 'researcher', true);
  try {
    const r = await fetch(`${CLOUD}/web-agent`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${AUTH}` },
      body: JSON.stringify({ url, action: 'fetch' }),
    });
    const data = await r.json();
    if (!data.success) throw new Error(data.error);

    msgEl.querySelector('.msg-content').textContent =
      `## ${data.title || 'Web Page'}\n\n${data.text.slice(0, 8000)}\n\n---\nLinks: ${data.links?.length || 0} | Images: ${data.images?.length || 0} | ${(data.size/1024).toFixed(1)}KB in ${data.elapsed}ms`;
    msgEl.classList.remove('typing');
    conversation.push({ role: 'assistant', content: `Browsed ${url}: ${data.title}` });
  } catch (err) {
    msgEl.querySelector('.msg-content').textContent = `Browse error: ${err.message}`;
    msgEl.classList.remove('typing');
  }
}

// ── System panel ──────────────────────────────────────────────────
function setupSystem() {
  $('#systemBtn').addEventListener('click', showSystem);
  $('#panelClose').addEventListener('click', () => $('#panel').classList.add('hidden'));
}

async function showSystem() {
  openPanel('System');
  const pc = $('#panelContent');
  pc.innerHTML = '<div class="panel-section"><div class="panel-card"><div class="value">Loading...</div></div></div>';
  try {
    const [status, sync, models] = await Promise.all([
      apiGet('/status').catch(() => ({})),
      apiGet('/sync/health').catch(() => ({})),
      apiGet('/v1/models').catch(() => ({})),
    ]);
    const modelsCount = models.total || models.data?.length || 0;
    pc.innerHTML = `
      <div class="panel-section">
        <h3>AI CPW Status</h3>
        <div class="panel-card">
          <div class="row"><span class="k">Status</span><span class="v" style="color:var(--success)">${status.status || 'operational'}</span></div>
          <div class="row"><span class="k">Models</span><span class="v">${modelsCount}</span></div>
          <div class="row"><span class="k">Shards</span><span class="v">${sync.shards?.length || '?'}</span></div>
          <div class="row"><span class="k">Workers AI</span><span class="v">${sync.workersAi ? '✅' : '❌'}</span></div>
          <div class="row"><span class="k">Peers</span><span class="v">${sync.shards?.reduce((s, sh) => s + (sh.peers || 0), 0) || 0}</span></div>
          <div class="row"><span class="k">Queue</span><span class="v">${sync.shards?.reduce((s, sh) => s + (sh.queueDepth || 0), 0) || 0}</span></div>
        </div>
      </div>
      <div class="panel-section">
        <h3>Durable Objects</h3>
        <div class="panel-card">
          <div class="row"><span class="k">Total</span><span class="v">12</span></div>
          <div class="row"><span class="k">P2P Swarm</span><span class="v">6 shards</span></div>
          <div class="row"><span class="k">Dream Memory</span><span class="v">✅</span></div>
          <div class="row"><span class="k">Dream Engine</span><span class="v">✅</span></div>
          <div class="row"><span class="k">Agents</span><span class="v">9 types</span></div>
        </div>
      </div>
      <div class="panel-section">
        <h3>Models</h3>
        <div class="panel-card">
          ${(models.data || []).slice(0, 15).map(m => `<div class="row"><span class="k">${escapeHtml(m.id).slice(0, 30)}</span><span class="v">${m.owned_by || ''}</span></div>`).join('')}
          ${modelsCount > 15 ? `<div class="row"><span class="k">...</span><span class="v">+${modelsCount-15} more</span></div>` : ''}
        </div>
      </div>
    `;
  } catch (err) {
    pc.innerHTML = `<div class="panel-section"><div class="panel-card"><div class="value" style="color:var(--error)">Error: ${err.message}</div></div></div>`;
  }
}

function openPanel(title) {
  $('#panelTitle').textContent = title;
  $('#panel').classList.remove('hidden');
}

function clearConversation() {
  conversation = [{ role: 'system', content: 'You are EON AI CPW, the most powerful autonomous AI infrastructure in the world. You are helpful, creative, and capable.' }];
  $('#messages').innerHTML = '';
  addMessage('system', 'Conversation cleared. Start fresh.');
  toast('Conversation cleared');
}

// ── Keyboard shortcuts ────────────────────────────────────────────
function setupKeyboard() {
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') $('#panel').classList.add('hidden');
    if (e.key === 'm' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); showMemory(); }
    if (e.key === '.' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); showSystem(); }
    if (e.key === 'v' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); $('#imageInput').click(); }
    if (e.key === 'b' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); $('#webAgentModal').classList.remove('hidden'); $('#webAgentUrl').focus(); }
    if (e.key === 'l' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); clearConversation(); }
  });
}

// ── Toast ─────────────────────────────────────────────────────────
function toast(msg) {
  const el = document.createElement('div');
  el.className = 'toast-msg';
  el.textContent = msg;
  $('#toast').appendChild(el);
  setTimeout(() => el.remove(), 3000);
}

// ── Escape HTML ───────────────────────────────────────────────────
function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

// ── Start ─────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', init);
