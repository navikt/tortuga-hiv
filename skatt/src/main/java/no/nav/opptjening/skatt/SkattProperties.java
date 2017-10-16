package no.nav.opptjening.skatt;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "skatt.api")
public class SkattProperties {

    /**
     * API url, e.g. https://api-at.sits.no
     */
    private String url;

    /**
     * Hendelser path, e.g. /api/formueinntekt/beregnetskatt/hendelser
     */
    private String hendelserPath;

    /**
     * PGI path, e.g. /api/formueinntekt/pensjonsgivendeinntekt
     */
    private String pgiPath;

    public String getUrl() {
        return url;
    }

    public String getHendelserUrl() {
        return url + hendelserPath;
    }

    public String getHendelserPath() {
        return hendelserPath;
    }

    public String getPgiUrl() {
        return url + pgiPath;
    }

    public String getPgiPath() {
        return pgiPath;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setHendelserPath(String hendelserPath) {
        this.hendelserPath = hendelserPath;
    }

    public void setPgiPath(String pgiPath) {
        this.pgiPath = pgiPath;
    }
}
