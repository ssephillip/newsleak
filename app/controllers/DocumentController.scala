/*
 * Copyright (C) 2016 Language Technology Group and Interactive Graphics Systems Group, Technische Universität Darmstadt, Germany
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package controllers

import javax.inject.Inject
import models.services.DocumentService
import models.{ Document, Facets, IteratorSession }
import models.KeyTerm.keyTermFormat
import models.Tag.tagFormat
import play.api.cache.CacheApi
import play.api.libs.json.{ JsObject, JsValue, Json }
import play.api.mvc.{ Action, AnyContent, Controller, Request }
import util.{ DateUtils, NewsleakConfigReader }
import util.SessionUtils.currentDataset
import scala.collection.mutable.ListBuffer
import scalaj.http.Http
import scalaj.http.HttpResponse

/**
 * Provides document related actions.
 *
 * @param cache the application cache.
 * @param documentService the service for document backend operations.
 * @param dateUtils common helper for date and time operations.
 */
class DocumentController @Inject() (
    cache: CacheApi,
    documentService: DocumentService,
    dateUtils: DateUtils
) extends Controller {

  private val defaultPageSize = 50

  /**
   * Returns a list of documents associated with the given tag label.
   *
   * @param label the tag label to search for.
   * @return a list of documents associated with the given tag label.
   */
  def getDocsByLabel(label: String) = Action { implicit request =>
    val docs = documentService.getByTagLabel(label)(currentDataset)
    Ok(createJsonResponse(docs, docs.length))
  }

  /**
   * Returns the IDs of similar documents for a given ID.
   * The IDs are retrieved from the vector index.
   * @param id the ID of the document for which similar documents shall be retrieved
   * @param numOfDocs the number of IDs that shall be retrieved
   * @return the IDs of similar documents and the corresponding similarity scores (cosine distances) that were retrieved from the vector index
   */
  def getSimDocsDoc2VecC(id: String, numOfDocs: Int) = Action { implicit request =>
    val address = NewsleakConfigReader.config.getString("vectorindex.address")

    val urlString: String = address + "/vector/" + id + "?num=" + numOfDocs

    // executes an Http get request
    val response: HttpResponse[String] = Http(urlString).asString
    val bodyAsJson: JsValue = Json.parse(response.body)

    Ok(bodyAsJson).as("application/json")
  }

  /**
   * Returns an elasticsearch GetRequest Entities Field response.
   *
   * @param id the document id.
   * @return a list of documents associated with the given tag label.
   */
  def getESEntitiesByDoc(id: String) = Action { implicit request =>
    val entities = documentService.getDocumentEntities(id)(currentDataset)
    var response = ""

    if (Option(entities) == None) {
      response = "None"
    } else response = "Some"

    Ok(Json.obj("option" -> response)).as("application/json")
  }

  /**
   * Returns an elasticsearch GetRequest Entities Field response.
   *
   * @param id the document id.
   * @return a list of documents associated with the given tag label.
   */
  def getKeywordsByDoc(id: String) = Action { implicit request =>
    val entities = documentService.getDocumentKeywords(id)(currentDataset)
    var response = ""

    if (Option(entities) == None) {
      response = "None"
    } else response = "Some"

    Ok(Json.obj("option" -> response)).as("application/json")
  }

  /**
   * Returns an elasticsearch GetRequest Entities Field response.
   *
   * @param id the document id.
   * @return a list of documents associated with the given tag label.
   */
  def getEntitiesTypeByDoc(id: String, entType: String) = Action { implicit request =>
    val entities = documentService.getEntitiesType(id, entType)(currentDataset)
    var response = ""

    if (Option(entities) == None) {
      response = "None"
    } else response = "Some"

    Ok(Json.obj("option" -> response)).as("application/json")
  }

  /** Returns elasticsearch UpdateRequest response. */
  def retrieveKeywords(docId: String) = Action { implicit request =>
    val keywords = documentService.getKeywordsInES(docId)(currentDataset)
    var res = Array[JsValue]()

    var i = 0
    val l = keywords.length

    while (i < l) {

      var kwd = keywords(i).asInstanceOf[List[_]](0).toString
      var term = keywords(i).asInstanceOf[List[_]](1).toString

      res = res :+ Json.obj("Keyword" -> kwd, "TermFrequency" -> term)
      i += 1
    }

    Ok(Json.obj("keys" -> res)).as("application/json")
  }

  /** Returns elasticsearch UpdateRequest response. */
  def createInitEntity(docId: String, entId: Int, entName: String, entType: String) = Action { implicit request =>
    val entities = documentService.buildInitEntity(docId, entId, entName, entType)(currentDataset)
    var response = ""

    if (Option(entities) == None) {
      response = "None"
    } else response = "Some"

    Ok(Json.obj("option" -> response)).as("application/json")
  }

  /** Returns elasticsearch UpdateRequest response. */
  def createNewEntity(docId: String, entId: Int, entName: String, entType: String) = Action { implicit request =>
    val entities = documentService.buildNewEntity(docId, entId, entName, entType)(currentDataset)
    var response = ""

    if (Option(entities) == None) {
      response = "None"
    } else response = "Some"

    Ok(Json.obj("option" -> response)).as("application/json")
  }

  /** Returns elasticsearch UpdateRequest response. */
  def createInitKeyword(docId: String, keyword: String) = Action { implicit request =>
    val key = documentService.buildInitKeyword(docId, keyword)(currentDataset)
    var response = ""

    if (Option(key) == None) {
      response = "None"
    } else response = "Some"

    Ok(Json.obj("option" -> response)).as("application/json")
  }

  /** Returns elasticsearch UpdateRequest response. */
  def createNewKeyword(docId: String, keyword: String) = Action { implicit request =>
    val key = documentService.buildNewKeyword(docId, keyword)(currentDataset)
    var response = ""

    if (Option(key) == None) {
      response = "None"
    } else response = "Some"

    Ok(Json.obj("option" -> response)).as("application/json")
  }

  /** Returns elasticsearch UpdateRequest response. */
  def createInitEntityType(docId: String, entId: Int, entName: String, entType: String) = Action { implicit request =>
    val entities = documentService.buildInitEntityType(docId, entId, entName, entType)(currentDataset)
    var response = ""

    if (Option(entities) == None) {
      response = "None"
    } else response = "Some"

    Ok(Json.obj("option" -> response)).as("application/json")
  }

  /** Returns elasticsearch UpdateRequest response. */
  def createNewEntityType(docId: String, entId: Int, entName: String, entType: String) = Action { implicit request =>
    val entities = documentService.buildNewEntityType(docId, entId, entName, entType)(currentDataset)
    var response = ""

    if (Option(entities) == None) {
      response = "None"
    } else response = "Some"

    Ok(Json.obj("option" -> response)).as("application/json")
  }

  // TODO: Extend ES API and remove KeyTerm API
  /**
   * Returns important terms occurring in the document content for the given document id.
   *
   * @param id the document id.
   * @param size the number of terms to fetch.
   * @return a list of terms representing important keywords for the given document.
   */
  def getKeywordsById(id: Int, size: Int) = Action { implicit request =>
    val terms = documentService.getKeywords(id, Some(size))(currentDataset)
    Ok(Json.toJson(terms)).as("application/json")
  }

  /** Returns all distinct annotated document labels. */
  def getTagLabels() = Action { implicit request =>
    Ok(Json.obj("labels" -> Json.toJson(documentService.getDocumentLabels()(currentDataset)))).as("application/json")
  }

  /** Returns index name. */
  def getIndexName() = Action { implicit request =>
    Ok(Json.obj("index" -> Json.toJson(documentService.getIndex()(currentDataset)))).as("application/json")
  }

  /**
   * Annotates a document with the given label.
   *
   * @param id the document id to annotate.
   * @param label the label to assign.
   * @return the added tag or if already present the existing tag.
   */
  def addTag(id: Int, label: String) = Action { implicit request =>
    Ok(Json.obj("id" -> documentService.addTag(id, label)(currentDataset).id)).as("application/json")
  }

  /**
   * Removes the tag from the document associated with the given id.
   *
   * @param tagId the tag id associated with a document.
   */
  def removeTagById(tagId: Int) = Action { implicit request =>
    documentService.removeTag(tagId)(currentDataset)
    Ok("success").as("Text")
  }

  /**
   * Returns a list of tags associated with the given document id.
   *
   * @param id the document id.
   * @return a list of tags.
   */
  def getTagsByDocId(id: Int) = Action { implicit request =>
    val tags = documentService.getTags(id)(currentDataset)
    Ok(Json.toJson(tags)).as("application/json")
  }

  /**
   * Search for documents given a search query.
   *
   * @param fullText match documents that contain the given expression in the document body.
   * @param generic a map linking from document metadata keys to a list of instances for this metadata.
   * @param entities a list of entity ids that should occur in the document.
   * @param timeRange a string representing a time range for the document creation date.
   * @param timeExprRange a string representing a time range for the document time expression.
   * @return list of matching documents with their metadata.
   */
  def getDocs(
    fullText: List[String],
    generic: Map[String, List[String]],
    entities: List[Long],
    keywords: List[String],
    timeRange: String,
    timeExprRange: String
  ) = Action { implicit request =>
    val uid = request.session.get("uid").getOrElse("0")
    val (from, to) = dateUtils.parseTimeRange(timeRange)
    val (timeExprFrom, timeExprTo) = dateUtils.parseTimeRange(timeExprRange)
    val facets = Facets(fullText, generic, entities, keywords, from, to, timeExprFrom, timeExprTo)
    var pageCounter = 0

    val cachedIterator = cache.get[IteratorSession](uid)
    // Initial page load or filter applied
    val iteratorSession = if (cachedIterator.isEmpty || cachedIterator.forall(_.hash != facets.hashCode())) {
      val (numDocs, it) = documentService.searchDocuments(facets, defaultPageSize)(currentDataset)
      val session = IteratorSession(numDocs, it, facets.hashCode())
      cache.set(uid, session)
      session
      // Document list scrolled
    } else {
      cachedIterator.get
    }

    val docList = ListBuffer[Document]()
    while (iteratorSession.hitIterator.hasNext && pageCounter <= defaultPageSize) {
      docList += iteratorSession.hitIterator.next()
      pageCounter += 1
    }

    if (docList.size < defaultPageSize) {
      val newIteratorSession = IteratorSession(iteratorSession.hits, iteratorSession.hitIterator, -1)
      cache.set(uid, newIteratorSession)
    }

    Ok(createJsonResponse(docList.toList, iteratorSession.hits))
  }

  /**
   * Gets the ids of similar documents with the Elasticsearch "more like this" query
   * @param id the document ID of the document for which similar documents shall be retrieved
   * @param numOfDocs the number of similar documents that shall be retrieved
   * @return the ids of the similar documents and the similarity scores
   */
  def getMoreLikeThis(id: String, numOfDocs: Int) = Action { implicit request =>

    val idsAndScores = documentService.searchMoreLikeThis(id, numOfDocs)(currentDataset);

    Ok(Json.toJson(idsAndScores)).as("application/json")
  }

  /**
   * query full document using a list of document ids
   * @param ids List of Document Ids
   * @return list of matching documents with their metadata.
   */
  def getDocsByIds(ids: List[Long]) = Action { implicit request =>
    val docs = ids.flatMap(x => documentService.getById(x)(currentDataset))
    Ok(createJsonResponse(docs, ids.size))
  }

  private def createJsonResponse(docList: List[Document], hits: Long)(implicit request: Request[AnyContent]): JsValue = {
    if (docList.nonEmpty) {
      val keys = documentService.getMetadataKeys()(currentDataset)
      val docToMetadata = documentService
        .getMetadata(docList.map(_.id), keys)(currentDataset)
        .groupBy(_._1)
        .map {
          case (id, l) =>
            id -> l.collect {
              case (_, k, v, t) if v != null && !v.isEmpty => {
                // println(k);
                Json.obj("key" -> k, "val" -> v, "type" -> t)
              }
            }
        }

      val response = docList.map { d =>
        Json.obj(
          "id" -> d.id,
          "content" -> d.content,
          "highlighted" -> d.highlightedContent,
          "metadata" -> docToMetadata.get(d.id)
        )
      }

      Json.obj("hits" -> hits, "docs" -> response)
    } else {
      Json.obj("hits" -> 0, "docs" -> List[JsObject]())
    }
  }
}
