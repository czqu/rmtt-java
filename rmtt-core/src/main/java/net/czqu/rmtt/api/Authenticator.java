package net.czqu.rmtt.api;

/**
 * Application-injected authentication policy. The server asks this on every CONNECT; the caller
 * decides how to interpret the credential (JWT / bare device id / user+password / ...).
 *
 * <p><b>Threading:</b> {@link #authenticate(String)} is invoked inline on the transport I/O
 * thread, so implementations must not block (remote lookups should be offloaded by the caller).
 * When credentials are compared against stored secrets, use a constant-time comparison
 * (e.g. {@code MessageDigest.isEqual}) rather than {@code String.equals}.</p>
 */
@FunctionalInterface
public interface Authenticator {

    /**
     * Authenticate a CONNECT credential.
     *
     * @param credential the credential carried by the CONNECT message
     * @return {@link AuthResult#allow} to accept, any other result to reject
     */
    AuthResult authenticate(String credential);
}