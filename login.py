import streamlit as st

st.set_page_config(
    initial_sidebar_state="collapsed",
    page_title="HiBlooms — Acceso",
    layout="wide"
)

# Inject CSS theme + login page styles
with open("styles.css", "r", encoding="utf-8") as _f:
    st.markdown(f"<style>{_f.read()}</style>", unsafe_allow_html=True)

st.markdown("""
<style>
  [data-testid="stSidebarNav"] { display: none; }

  /* Center the login card */
  .login-wrap {
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 80vh;
  }
  .login-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: var(--radius-xl);
    padding: 3rem 3rem 2.5rem;
    width: 100%;
    max-width: 440px;
    position: relative;
    overflow: hidden;
  }
  .login-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, #00d4ff, #00e5b4, #00d4ff);
    background-size: 200% 100%;
    animation: shimmer 3s linear infinite;
  }
  @keyframes shimmer {
    0%   { background-position: 200% 0; }
    100% { background-position: -200% 0; }
  }
  .login-logo-area {
    text-align: center;
    margin-bottom: 2rem;
  }
  .login-title {
    font-family: 'Syne', sans-serif;
    font-size: 1.8rem;
    font-weight: 800;
    color: #e8f4ff;
    letter-spacing: -0.03em;
    margin: 0.75rem 0 0.25rem;
    line-height: 1;
  }
  .login-title span {
    background: linear-gradient(135deg, #00d4ff, #00e5b4);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }
  .login-sub {
    font-size: 12px;
    color: var(--text-secondary);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 0;
  }
  .login-divider {
    height: 1px;
    background: var(--border);
    margin: 1.5rem 0;
  }
  .login-footer {
    text-align: center;
    font-size: 11px;
    color: var(--text-muted);
    margin-top: 1.5rem;
    letter-spacing: 0.05em;
  }
  .sat-icon {
    font-size: 2.5rem;
    display: block;
    margin-bottom: 0.5rem;
    filter: drop-shadow(0 0 12px rgba(0,212,255,0.5));
  }
</style>
""", unsafe_allow_html=True)

def cargar_usuarios():
    usuarios = {}
    for key, value in st.secrets["auth"].items():
        if key.startswith("username"):
            idx = key.replace("username", "")
            pwd_key = f"password{idx}"
            if pwd_key in st.secrets["auth"]:
                usuarios[value] = st.secrets["auth"][pwd_key]
    return usuarios

users = cargar_usuarios()

query_params = st.query_params
admin_mode = query_params.get("admin", ["false"])[0].lower() == "true" if isinstance(query_params.get("admin", "false"), list) else str(query_params.get("admin", "false")).lower() == "true"

if admin_mode and not st.session_state.get("logged_in", False):
    st.session_state["logged_in"] = True
    st.switch_page("app.py")
    st.stop()

if st.session_state.get("logged_in", False):
    st.switch_page("app.py")
    st.stop()

# ── Layout: center column ──────────────────────────────────────
_, col, _ = st.columns([1, 1.2, 1])

with col:
    st.markdown("""
    <div class="login-card">
      <div class="login-logo-area">
        <span class="sat-icon">🛰</span>
        <div class="login-title">HI<span>BLOOMS</span></div>
        <div class="login-sub">Sistema de monitorización satelital</div>
      </div>
      <div class="login-divider"></div>
    </div>
    """, unsafe_allow_html=True)

    # The actual form sits below — styled via global CSS
    user = st.text_input("Usuario", placeholder="Introduce tu usuario")
    pwd  = st.text_input("Contraseña", type="password", placeholder="••••••••")
    submit = st.button("Iniciar sesión", use_container_width=True)

    st.markdown("""
    <div class="login-footer">
      PID2023-153234OB-I00 · Universidad de Navarra · BIOMA
    </div>
    """, unsafe_allow_html=True)

if submit:
    if user in users and pwd == users[user]:
        st.session_state["logged_in"] = True
        st.switch_page("app.py")
    else:
        st.error("Usuario o contraseña incorrectos")
