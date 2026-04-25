"""
app/auth.py — Minimal shared-password authentication for Streamlit.

Philosophy: this is an internal tool for 2-10 trusted team members.
Overkill = OAuth, SSO, user management. Right-sized = a shared password
stored as env var, session flag for "logged in".

For more users or per-user audit trails, replace this with streamlit-authenticator
or bolt on Google OAuth (~30 lines).
"""

import os
import hmac
import streamlit as st


def _check_password() -> bool:
    """Returns True if the user is authenticated. Otherwise renders the login form."""
    # Pull from env (set in Railway dashboard or .env)
    expected = os.environ.get("SAAS_PASSWORD", "")
    if not expected:
        # If no password configured, fail open in dev only
        if os.environ.get("SAAS_ALLOW_NO_AUTH") == "1":
            st.session_state["authed"] = True
            st.session_state["user"] = "dev"
            return True
        st.error(
            "⚠️ SAAS_PASSWORD env var not set. Refusing to run unauthenticated. "
            "Set SAAS_PASSWORD in your deployment, or set SAAS_ALLOW_NO_AUTH=1 for local dev."
        )
        return False

    if st.session_state.get("authed"):
        return True

    with st.container():
        st.markdown("### SaaS for Blog")
        st.caption("Divinheal content pipeline — internal tool")
        with st.form("login"):
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Sign in")
        if submit:
            if hmac.compare_digest(password, expected):
                st.session_state["authed"] = True
                st.session_state["user"] = "internal"
                st.rerun()
            else:
                st.error("Wrong password")
        return False


def require_auth():
    """Call at the top of every Streamlit page. Blocks rendering if not authed."""
    if not _check_password():
        st.stop()


def current_user() -> str:
    return st.session_state.get("user", "anonymous")


def logout():
    for k in ("authed", "user"):
        if k in st.session_state:
            del st.session_state[k]
    st.rerun()
