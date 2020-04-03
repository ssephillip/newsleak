package uhh_lt.newsleak.types;

import java.util.Objects;

/**
 * A TpDocument represents an inner document of the Transparenz Portal solr index.
 * The solr index of the Transparenz Portal contains SolrDocuments (outer documents).
 * An outer document is only a wrapper for the information of the actual document.
 * This "actual document" is an inner document.
 * An outer document may hold information about multiple inner documents.
 * An outer document holds many lists with different information (e.g. file-formats, filename, fultexts).
 * Each element in the list is the information of one inner document.
 * The information is mapped to the inner document by the position in the list.
 * E.g. For the inner document X the filename is in the position 2 of the list of filenames.
 *      Therefore the file-format in position 2 in the list of file-formats is the file-format of this inner document etc.
 */
public class TpResource {

    /** The id of the outer document (possibly referencing multiple TpDocuments (inner documents)) */
    String outerId;

    /** The id of the inner document (represented by this object). */
    String innerId; //TODO ps 2019-08-20: evtl. auf int umstellen

    /** The concatenation of the outer id and the inner id */
    String id;

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

    /** The date the document was published */
    String date;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TpResource)) return false;
        TpResource that = (TpResource) o;
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }



}
