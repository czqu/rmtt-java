package net.czqu.rmtt.api;

/**
 * Application-injected authentication policy. The server asks this on every CONNECT; the caller
 * decides how to interpret the credential (JWT / bare device id / user+password / ...).
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