package uhh_lt.newsleak.types;

import java.util.Objects;

public class TpDocument {

    String tpId;
    String newsleakId;
    String resUrl;
    String resFulltext;
    String resFormat;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TpDocument)) return false;
        TpDocument that = (TpDocument) o;
        return Objects.equals(newsleakId, that.newsleakId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(newsleakId);
    }

    public String getTpId() {
        return tpId;
    }

    public void setTpId(String tpId) {
        this.tpId = tpId;
    }

    public String getNewsleakId() {
        return newsleakId;
    }

    public void setNewsleakId(String newsleakId) {
        this.newsleakId = newsleakId;
    }

    public String getResUrl() {
        return resUrl;
    }

    public void setResUrl(String resUrl) {
        this.resUrl = resUrl;
    }

    public String getResFulltext() {
        return resFulltext;
    }

    public void setResFulltext(String resFulltext) {
        this.resFulltext = resFulltext;
    }

    public String getResFormat() {
        return resFormat;
    }

    public void setResFormat(String resFormat) {
        this.resFormat = resFormat;
    }



}
