package uhh_lt.newsleak.types;

import java.util.Objects;

public class TpDocument {

    /** The id of the outer document (possibly referencing multiple TpDocuments (inner documents)) */
    String outerId;

    /** The id of the inner document (represented by this object). */
    String innerId;

    /** The URL to the original resource. */
    String resUrl;

    /** The fulltext of this (inner) document. */
    String resFulltext;

    /** The format of this (inner) document. */
    String resFormat;


    /** The outer document name. */
    String title;

    /** The inner document name */
    String resName;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TpDocument)) return false;
        TpDocument that = (TpDocument) o;
        return Objects.equals(innerId, that.innerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(innerId);
    }

    public String getOuterId() {
        return outerId;
    }

    public void setOuterId(String outerId) {
        this.outerId = outerId;
    }

    public String getInnerId() {
        return innerId;
    }

    public void setInnerId(String innerId) {
        this.innerId = innerId;
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


    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getResName() {
        return resName;
    }

    public void setResName(String resName) {
        this.resName = resName;
    }



}
