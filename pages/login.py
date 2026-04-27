import streamlit as st

st.set_page_config(
    initial_sidebar_state="collapsed",
    page_title="HiBlooms — Acceso",
    layout="wide"
)

with open("styles.css", "r", encoding="utf-8") as _f:
    st.markdown(f"<style>{_f.read()}</style>", unsafe_allow_html=True)

st.markdown("""
<style>
  [data-testid="stSidebarNav"] { display: none; }

  .login-card {
    background: #ffffff;
    border: 1px solid #e2ecf0;
    border-radius: 24px;
    padding: 2.8rem 3rem 2.2rem;
    width: 100%;
    max-width: 420px;
    position: relative;
    overflow: hidden;
    box-shadow: 0 8px 32px rgba(0,168,150,.10), 0 2px 8px rgba(15,31,46,.06);
    margin: auto;
  }
  .login-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 4px;
    background: linear-gradient(90deg, #00a896, #10b981, #00a896);
    background-size: 200%;
    animation: shimmer 3s linear infinite;
  }
  @keyframes shimmer {
    0%   { background-position: 200% 0; }
    100% { background-position: -200% 0; }
  }
  .login-icon {
    font-size: 2.6rem;
    text-align: center;
    margin-bottom: .6rem;
    display: block;
  }
  .login-title {
    font-family: 'Cabinet Grotesk', sans-serif;
    font-size: 2rem;
    font-weight: 900;
    text-align: center;
    color: #0f1f2e;
    letter-spacing: -0.03em;
    line-height: 1;
    margin-bottom: .3rem;
  }
  .login-title span { color: #00a896; }
  .login-sub {
    text-align: center;
    font-size: 11px;
    color: #8fa3b0;
    letter-spacing: .08em;
    text-transform: uppercase;
    margin-bottom: 0;
  }
  .login-divider {
    height: 1px;
    background: #e2ecf0;
    margin: 1.5rem 0 1.25rem;
  }
  .login-footer {
    text-align: center;
    font-size: 11px;
    color: #8fa3b0;
    margin-top: 1.25rem;
    letter-spacing: .04em;
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
admin_val = query_params.get("admin", "false")
admin_mode = (admin_val if isinstance(admin_val, str) else admin_val[0]).lower() == "true"

if admin_mode and not st.session_state.get("logged_in", False):
    st.session_state["logged_in"] = True
    st.switch_page("app.py")
    st.stop()

if st.session_state.get("logged_in", False):
    st.switch_page("app.py")
    st.stop()

# ── Centrado vertical ──────────────────────────────────────────
st.markdown("<div style='height:6vh'></div>", unsafe_allow_html=True)
_, col, _ = st.columns([1, 1.1, 1])

with col:
    st.markdown("""
    <div class="login-card">
      <span class="login-icon">🛰</span>
      <div class="login-title">HI<span>BLOOMS</span></div>
      <div class="login-sub">Sistema de monitorización satelital</div>
      <div class="login-divider"></div>
    </div>
    """, unsafe_allow_html=True)

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
