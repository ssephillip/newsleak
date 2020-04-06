package uhh_lt.newsleak.types;

import java.util.Objects;

/**
 * A TpResource represents a resource of the Transparenzportal Solr Index.
 *
 * In the following we will first explain how the data in the Transparenzportal is organized.
 * Then we will explain how this structure transfers to this application.
 *
 * <h3>The data in the Transparenzportal is organized as follows:</h3>
 * The data in the Transparenzportal is published in the form of datasets.
 * A dataset contains information such as the title of the dataset, the publishing date of the dataset,  a unique ID to identify the dataset etc.
 * Additionally, a dataset contains one or more resources. The purpose of a dataset is to group closely related resources or identical resources of different file formats or representations.
 * A resource represents an actual file (e.g. PDf-File, CSV-file).
 * A resource consists of metadata such as the name of the resource and the file format of the actual file respresented by this resource (in the following called resource format).
 * The metadata of the resources in a dataset are not grouped by resources but by type of metadata.
 * This means that the dataset contains multiple lists. Each list contains one type of information for all the resources in the dataset.
 * For example a dataset contains a list of resource names (res_name) and a list of resource formats (res_formats).
 * Each list item in the list "res_name" is the name of one of the resources in the dataset.
 * Each list item in the list "res_format" is the resource format of one of the resources in the dataset.
 * The information of one resource is in the same position in all the lists.
 * For example if the name of a resource is at position 2 in the list "res_name", the resource format of this resource is also at position 2 in the list "res_format".
 * Resources do not have their own ID.
 * To extract a resource from a dataset, we need to collect the information of the resource by going through all the lists.
 *
 *
 * <h3>Implementation</h3>>
 * In this application, a dataset is represented by objects of the class {@link org.apache.solr.common.SolrDocument}.
 * A resource is represented by objects of this class ({@link TpResource}).
 * As mentioned above, resources do not have their own ID.
 * However we need to have unique resource IDs to be able to reference resources directly.
 * For example we want to be able to get a specific resource from the Transparenzportal.
 *
 * To solve this problem we can leverage the list position of the resources.
 * We can use the list position as relative ID of a resource within the dataset.
 * This relative ID of a resource is represented by the field {@link #relativeResourceId}.
 * By concatenating the {@link #datasetId} and the {@link #relativeResourceId} we obtain a unique ID with which we can directly reference resources.
 * This concatenation is represented by the field {@link #absoluteResourceId}.
 * The {@link #absoluteResourceId} is of the form:
 * {@link #datasetId}_{@link #relativeResourceId}
 *
 * When extracting a resource from a dataset it is important to ensure that the daataset is well-formed.
 * We consider a dataset to be well-formed if it contains all the metadata that we consider mandatory for all the resources in the dataset.
 *
 * In practice this means that a dataset must contain:
 * 1. the Dataset ID
 * 2. the list of URLs to the actual files represented by the resources
 * 3. the list of resource formats
 * 4. the list of resources names
 * And in some cases:
 * 5. the list of resource fulltexts
 *
 * All the mandatory lists need to be of the same length (otherwise a mandatory metadata value would be missing for at least one resource).
 * A dataset that is NOT "well-formed" MUST be discarded to ensure the consistency of the application.
 *
 * One may ask: "If a mandatory value is only missing for one resource, why do we need to discard the whole dataset and not just this one resource?". *
 * The answer to this is:
 * If one of the lists has a different length, it is not possible anymore to determine which metadata belongs to which resource.
 *
 * Example:
 * A dataset contains three resources. The information of resource-1 is at position 0, the information of resource-2 is at position 1 and the information of resource-3 is at position 2 in the lists.
 * If the list "res_format" only contains two values (instead of three) we would assign the value at position 0 to resource-1 and the value at position 1 to resource-2.
 * However, there is no way of telling if this is correct.
 * It might be, that the resource format of resource-2 is missing and the resource format at position 1 is the resource format of resource-3.
 * In this case, the resource format of resource-3 would be assigned to resource-2, leading to inconsistencies and wrong data throughout the application.
 */
public class TpResource {

    /** The id of the dataset */
    String datasetId;

    /** The relative resource ID. See class description for more information on resources and datasets. */
    String relativeResourceId;

    /** The absolute resource ID. It is the concatenation of the dataset ID and the relative resource ID. */
    String absoluteResourceId;

    /** The URL to the actual file. */
    String url;

    /** The fulltext of this resource. */
    String fulltext;

    /** The format of this resource. More precisely: The format of the actual file that is represented by this resource. */
    String format;


    /** The dataset title. */
    String datasetTitle;

    /** The resource name. */
    String name;

    /** The date on which the dataset was published */
    String datasetDate;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TpResource)) return false;
        TpResource that = (TpResource) o;
        return Objects.equals(absoluteResourceId, that.absoluteResourceId);
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
