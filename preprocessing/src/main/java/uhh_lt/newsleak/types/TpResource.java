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
    String datasetId;

    /** The id of the inner document (represented by this object). */
    String relativeResourceId; //TODO ps 2019-08-20: evtl. auf int umstellen

    /** The concatenation of the outer id and the inner id */
    String absoluteResourceId;

    /** The URL to the original resource. */
    String url;

    /** The fulltext of this (inner) document. */
    String fulltext;

    /** The format of this (inner) document. */
    String format;


    /** The dataset title. Corresponds to the field "title" in the TP Solr Index. */
    String datasetTitle;

    /** The resource name. Corresponds to the field "res_name" in the TP Solr Index. TODO warsch. in title umbenennen für konsistenz innerhalb newsleaks (noch nicht gemacht um verwirrung mit dem alten feld "title" zu verhindern */
    String name;

    /** The date the document was published */
    String datasetDate;

    //TODO hier muss die absolute id verwendet werden
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TpResource)) return false;
        TpResource that = (TpResource) o;
        return Objects.equals(relativeResourceId, that.relativeResourceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relativeResourceId);
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getRelativeResourceId() {
        return relativeResourceId;
    }

    public void setRelativeResourceId(String relativeResourceId) {
        this.relativeResourceId = relativeResourceId;
    }

    public String getAbsoluteResourceId() {
        return absoluteResourceId;
    }

    public void setAbsoluteResourceId(String absoluteResourceId) {
        this.absoluteResourceId = absoluteResourceId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getFulltext() {
        return fulltext;
    }

    public void setFulltext(String fulltext) {
        this.fulltext = fulltext;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }


    public String getDatasetTitle() {
        return datasetTitle;
    }

    public void setDatasetTitle(String datasetTitle) {
        this.datasetTitle = datasetTitle;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDatasetDate() {
        return datasetDate;
    }

    public void setDatasetDate(String datasetDate) {
        this.datasetDate = datasetDate;
    }



}
