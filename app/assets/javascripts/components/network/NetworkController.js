/*
 * Copyright 2016 Technische Universitaet Darmstadt
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

define([
    'angular',
    'angularMoment',
    'd3',
    'awesome-slider',
    'toggle-switch'
], function (angular) {
    'use strict';

    angular.module('myApp.network', ['play.routing', 'angularMoment', 'angularAwesomeSlider', 'toggle-switch']);
    angular.module('myApp.network')
        // This factory is used to share graph properties between this module and the app.js
        .factory('graphPropertiesShareService', function () {
            var graphProperties = {
                // Order: locations, orginaziations, persons, misc
                categoryColors: ["#8dd3c7", "#fb8072","#bebada", "#ffffb3"],
                categories:
                [
                	{id: 'LOC', full: 'Location'},
                	{id: 'ORG', full: 'Organization'},
                	{id: 'PER', full: 'Person'},
                	{id: 'MISC', full: 'Miscellaneous'}
                ],
                CATEGORY_COLOR_INDEX_LOC: 0,
                CATEGORY_COLOR_INDEX_ORG: 1,
                CATEGORY_COLOR_INDEX_PER: 2,
                CATEGORY_COLOR_INDEX_MISC: 3,
                // Delivers the index of a given category name
                getIndexOfCategory: function (category) {
                    switch (category) {
                        case 'LOC':
                            return this.CATEGORY_COLOR_INDEX_LOC;
                        case 'ORG':
                            return this.CATEGORY_COLOR_INDEX_ORG;
                        case 'PER':
                            return this.CATEGORY_COLOR_INDEX_PER;
                        default:
                            return this.CATEGORY_COLOR_INDEX_MISC;
                    }
                }
            };
            return graphProperties;
        })
        .controller('NetworkController',
        [
            '$scope',
            '$timeout',
            '$uibModal',
            '$window',
            'playRoutes',
            'appData',
            'moment',
            'graphPropertiesShareService',
            'highlightShareService',
            'uiShareService',
            'tagSelectShareService',
            'filterShareService',
            'ObserverService',
        function (
            $scope,
            $timeout,
            $uibModal,
            $window,
            playRoutes,
            appData,
            moment,
            graphPropertiesShareService,
            highlightShareService,
            uiShareService,
            tagSelectShareService,
            filterShareService,
            ObserverService
            )
        {

			$scope.filterShared = filterShareService;
            $scope.tagSelectShared = tagSelectShareService;
            $scope.graphShared = graphPropertiesShareService;
            $scope.uiShareService = uiShareService;

            $scope.observer = ObserverService;
            $scope.observer_subscribe = function(history) { $scope.history = history};
            $scope.observer.subscribeHistory($scope.observer_subscribe, "");

            const GRAVITATION_HEIGHT_SUBTRACT_VALUE = 126;

            $scope.maxNodeFreq = 592035;
            $scope.maxEdgeFreq = 81337;

            $scope.minNodeRadius = 15;
            $scope.maxNodeRadius = 30;
            $scope.minEdgeWidth  = 3;
            $scope.maxEdgeWidth  = 10;

            // determines the display of most or least frequency entities/edges
            // gets updated by toggle
            $scope.freqSorting = {least: false};

            var selectedNodes = new Array();
            $scope.selectedNodes = selectedNodes;
            var selectedEdges = new Array();
            var selectionColor = '#2A2AFF';

            $scope.merge={node: ""};


            // A slider for choosing the amount of displayed nodes from the type contries/cities.
            $scope.sliderCountriesCitiesAmount = {
                value: "3",
                options: {
                    from: 1,
                    to: 100,
                    step: 1,
                    dimension: " countries/cities",
                    limits: false,
                    scale: [1, 100],
                    css: {
                        background: {"background-color": "silver"},
                        before: {"background-color": "#7CB5EC"},
                        default: {"background-color": "silver"},
                        after: {"background-color": "#7CB5EC"},
                        pointer: {"background-color": "#2759AC"}
                    },
                    callback: function(value, released){
                        getGraph();
                    }
                }
            }
            // A slider for choosing the amount of displayed nodes from the type organization.
            $scope.sliderOrganizationsAmount = {
                value: "3",
                options: {
                    from: 1,
                    to: 100,
                    step: 1,
                    dimension: " organizations",
                    limits: false,
                    scale: [1, 100],
                    css: {
                        background: {"background-color": "silver"},
                        before: {"background-color": "#7CB5EC"},
                        default: {"background-color": "silver"},
                        after: {"background-color": "#7CB5EC"},
                        pointer: {"background-color": "#2759AC"}
                    },
                    callback: function(value, released){
                        getGraph();
                    }
                }
            }
            // A slider for choosing the amount of displayed nodes from the type person.
            $scope.sliderPersonsAmount = {
                value: "3",
                options: {
                    from: 1,
                    to: 100,
                    step: 1,
                    dimension: " persons",
                    limits: false,
                    scale: [1, 100],
                    css: {
                        background: {"background-color": "silver"},
                        before: {"background-color": "#7CB5EC"},
                        default: {"background-color": "silver"},
                        after: {"background-color": "#7CB5EC"},
                        pointer: {"background-color": "#2759AC"}
                    },
                    callback: function(value, released){
                        getGraph();
                    }
                }
            }
            // A slider for choosing the amount of displayed nodes from the type miscellaneous.
            $scope.sliderMiscellaneousAmount = {
                value: "3",
                options: {
                    from: 1,
                    to: 100,
                    step: 1,
                    dimension: " miscellaneous",
                    limits: false,
                    scale: [1, 100],
                    css: {
                        background: {"background-color": "silver"},
                        before: {"background-color": "#7CB5EC"},
                        default: {"background-color": "silver"},
                        after: {"background-color": "#7CB5EC"},
                        pointer: {"background-color": "#2759AC"}
                    },
                    callback: function(value, released){
                        getGraph();
                    }
                }
            }
            // A slider for choosing the minimum and maximum frequency of a displayed edge.
            $scope.sliderEdgeFrequency = {
                value: "1500;"+$scope.maxEdgeFreq,
                options: {
                    from: 1,
                    to: $scope.maxEdgeFreq,
                    step: 1,
                    dimension: " Connections between entities",
                    limits: false,
                    scale: [1, $scope.maxEdgeFreq],
                    css: {
                        background: {"background-color": "silver"},
                        range: {"background-color": "#7CB5EC"},
                        default: {"background-color": "silver"},
                        after: {"background-color": "#7CB5EC"},
                        pointer: {"background-color": "#2759AC"}
                    },
                    callback: function(value, released){
                        getGraph();
                    }
                }
            }

            // create a graph here
            // when the data changes, the graph remains the same,
            // but the visualization gets updated
            // http://bl.ocks.org/mbostock/1095795


            var nodes      = [];
            var edges      = [];
            var color      = d3.scale.category10().range($scope.graphShared.categoryColors);
            //console.log([color("LOC"), color("ORG"), color("PER"), color("MISC")])
            var radius     = d3.scale.sqrt()
                                    .domain([1,$scope.maxNodeFreq])
                                    .range([$scope.minNodeRadius, $scope.maxNodeRadius]);var radius     = d3.scale.sqrt()
                                    .domain([1,$scope.maxNodeFreq])
                                    .range([$scope.minNodeRadius, $scope.maxNodeRadius]);
            var edgeScale  = d3.scale.log()
                                    .domain([1,$scope.maxEdgeFreq])
                                    .range([$scope.minEdgeWidth,$scope.maxEdgeWidth]);

            // For zooming
            var currentScale = 1;
            $scope.zm      = d3.behavior.zoom()
                                    .scaleExtent([0.5, 3])
                                    .on('zoom', function(){
                                        force.start();
                                        currentScale = d3.event.scale;
                                        svg.selectAll('g').attr('transform', 'translate('
                                            + (-viewBoxX) + ',' + (-viewBoxY) + ')scale('
                                            + d3.event.scale + ')');
                                    });
            // For dragging
            var viewBoxX   = 0;
            var viewBoxY   = 0;
            var drag       = d3.behavior.drag()
                                    .on('drag', function(){
                                        force.start();
                                        viewBoxX -= d3.event.dx;
                                        viewBoxY -= d3.event.dy;
                                        svg.selectAll('g').attr('transform', 'translate('
                                            + (-viewBoxX) + ',' + (-viewBoxY) + ')scale('
                                            + currentScale + ')');
                                    });

            var svg;


            /**
             * This function updates the text with the name of the selected elements.
             */
            $scope.selectedElementsText = function (){
                var selectedElements = "";
                for(var i=0; i<selectedNodes.length; i++){
                    selectedElements += selectedNodes[i].name;
                    if(i < selectedNodes.length - 2)
                        selectedElements += ", ";
                    else if(i == selectedNodes.length - 2)
                        if(selectedEdges.length == 0)
                            selectedElements += " and ";
                        else
                            selectedElements += ", ";
                }
                if(selectedNodes.length != 0){
                    if(selectedEdges.length == 1)
                        selectedElements += " and ";
                    else if(selectedEdges.length > 1)
                        selectedElements += ", ";
                }
                for(var i=0; i<selectedEdges.length; i++){
                    selectedElements += selectedEdges[i].target.name + " <--> " + selectedEdges[i].source.name;
                    if(i < selectedEdges.length - 2)
                        selectedElements += ", ";
                    else if(i == selectedEdges.length - 2)
                        selectedElements += " and ";
                }
                return selectedElements;
            }


            /**
             * This function unselects all nodes. As a result, the words to highlight
             * have to be updated as well
             */
            function unselectNodes(){
                selectedNodes = new Array();
                d3.selectAll('circle').each(function(d){
                    d3.select(this).style('stroke-width', 1.5)
                                   .style('stroke', '#000000');
                });
                enableOrDisableButtons();
                // For the highlighting
                for (var i = 0; i < highlightShareService.wordsToHighlight.length; i++) {
                    highlightShareService.wordsToHighlight[i] = [];
                }
                highlightShareService.wasChanged = true;
            }


            /**
             * This function unselects all edges.
             */
            function unselectEdges(){
                selectedEdges = new Array();
                link.each(function(d){
                    d3.select(this).style('stroke', '#696969')
                                   .style('opacity', .8);
                    d3.select('#edgelabel_' + d.id).style('fill', '#000000')
                                                   .attr('font-weight', 'normal');
                });
                enableOrDisableButtons();
            }

            /**
             * mark the specified node as selected
             *
             * @param node
             * 		the node to select
             */
            function selectNode(node)
			{
				selectedNodes.push(node);

				d3.select("#nodecircle_" + node.id)
					.style('stroke-width', 5)
              		.style('stroke', selectionColor);

              	// Add the selected node name to the words that get highlighted
                // However, add it only once to make deletion of that entry easier
                if (highlightShareService.wordsToHighlight[$scope.graphShared.getIndexOfCategory(node.type)].indexOf(node.name) < 0) {
                     highlightShareService.wordsToHighlight[$scope.graphShared.getIndexOfCategory(node.type)].push(node.name);
                     highlightShareService.wasChanged = true;
                }
            }

            /**
             * mark the specified node as not
             * selected
             *
             * @param node
             *		the node to unselect
             */
            function unselectNode(node)
            {
            	selectedNodes.splice(selectedNodes.indexOf(node));
            	d3.select("#nodecircle_" + node.id)
            		.style('stroke-width', 1.5)
                    .style('stroke', '#000000');

                // Remove the selected node name from the words that get highlighted
                var index = highlightShareService.wordsToHighlight[$scope.graphShared.getIndexOfCategory(node.type)].indexOf(node.name);
                if (index > -1) {
                    highlightShareService.wordsToHighlight[$scope.graphShared.getIndexOfCategory(node.type)].splice(index, 1);
                    highlightShareService.wasChanged = true;
                }
            }

            /**
             * paints a circle lowered with the specific size
             *
             * @param node
             * 		the node to reduce
             * @param value
             *		the value from which so reduce
             */
            function reduceNode(node, value)
            {
            	d3.select("#node_" + node.id)
            }


            /**
             * get the nodes associated with this specific name
             *
             * @param name
             *		the name to search for
             */
            function getNodesByName(name)
            {
            	return nodes.filter(
            	function(element, index, array)
            	{
            		return element.name == name;
            	});
            }

            function getNodeById(id)
            {
            	return nodes.filter(
            		function(element, index, array)
            		{
            			return element.id === id;
            		}
            	)[0];
            }

			/**
			 *	get all ego networks associated with this specific
			 *  node
			 *
			 *	@param name
			 *		the name of which to search for search terms
			 *	@param callback
			 *		callback called when all ego networks are ready
			 */
            function getEgoNetworkByName(name, callback)
            {
               	playRoutes.controllers.NetworkController.getIdsByName(name).get().then(function(response)
               	{
               		response.data.ids.forEach(function(id){getEgoNetworkById(id, callback);});
               	});
            }


			/**
			 *	add an annotation to the specified Node
			 *
			 *	@param node
			 *		the node to attach the annotation
			 *	@param text
			 *		the text to attach to the annotation
			 */
            function addAnnotation(node, text)
            {
            	d3.select("#node_" + node.id)
            		.append('foreignObject')
            		.attr('width', '24')
            		.attr('height', '24')
            		.attr('x', function(d){return radius(d.freq)-2;})
            		.attr('y', /*function(d){return (radius(d.freq));}*/-12)
            		.append('xhtml:body')
            		.html('<button type="button" class="btn btn-xs btn-default neighbor-button" ng-show="!isViewLoading"><i class="glyphicon glyphicon-comment"></i></button>')
            		.on('mouseup', function(){alert(text);});

                //track annotating nodes within observer
                $scope.observer.addItem({
                    type: 'annotate',
                    data: {
                        id: node.id,
                        name: '<'+node.name+'-'+text+'>',
                        text: text
                    }
                });
				//alert("Annotation added");
            }


            // create new force layout
            var force;
            var link;
            var node;
            var edgepaths;
            var edgelabels;

            //TODO: refactor from uiShare to onresize angular material
            /**
             * Calculates and sets the new gravitation figures for the graph.
             */
            function calculateNewForceSize() {
                if (force != undefined) {
                    force.size([
                        $scope.uiShareService.mainContainerWidth,
                        $scope.uiShareService.mainContainerHeight
                    ]);
                    force.start();
                }
            }


            /**
             * Set the height of the legend popover.
             */
            function setLegendPopoverHeight() {
                var legendPopoverHeight = $scope.uiShareService.mainContainerHeight - 140;
                $('.popover-content > .legend-actual-content').css('max-height', legendPopoverHeight);
            }

            /**
             * Whenever the size of the graph container is changed, the gravitation parameter is updated.
             * At the moment, the entire graph needs to be redrawn in order for the changes to take effect.
             * TODO: There might be a better solution without redrawing the entire graph.
             * Also, the height of the popover is updated.
             */
            $scope.$watch('uiShareService', function() {
                calculateNewForceSize();
                //setLegendPopoverHeight();
            }, true);

            angular.element($window).bind('resize', function () {
                calculateNewForceSize();
            });

        	/**
			 * whenever a new tag is written into the search bar, we tell
			 * the graph to mark the nodes and if not available, we create
			 * the associated ego networks and mark them
			 */
            $scope.$watch('tagSelectShared.wasChanged', function(){
                if($scope.tagSelectShared.wasChanged)
                {
                	//select all nodes which have to be selected
					var tags = $scope.tagSelectShared.tagsToSelect;
					for(var i=0; i<tags.length; i++)
					{
						var nodes = getNodesByName(tags[i]);

						if(nodes.length == 0)
						{
							//get all ego networks and after that execute a callback function
							//which marks all selected nodes
							getEgoNetworkByName(tags[i],
							(function()
							{
								var tag = tags[i];
								return function(){
								var tagnodes = getNodesByName(tag);
								tagnodes.forEach(function(node){selectNode(node);})};
							})());
						}
						else
						{
							//mark all existing nodes
							nodes.forEach(function(node){selectNode(node);})
						}
					}

					//unselect all nodes which should be unselected
					tags = $scope.tagSelectShared.tagsToUnselect;
					for(var i=0; i<tags.length; i++)
					{
						var nodes = getNodesByName(tags[i]);
						//unmark all nodes
                        nodes.forEach(function(node){unselectNode(node);})
					}
					$scope.tagSelectShared.wasChanged = false;
					enableOrDisableButtons();
                }
            });


            /*
             * This function creates the SVG where the graph will be created in.
             */
            function createSVG(){
                d3.select('#network-graph').select("svg").remove();
                svg = d3.select('#network-graph')
                    .append('svg')
                    .attr('width', '100%')
                    .attr('height', '100%')
                    .append('g')
                    .call($scope.zm)
                    .on('dblclick.zoom', null)  // Disable doubleclicking zoom.
                    // Disable the call of the zoom-function when clicking and moving.
                    .on('mousedown.zoom', null)
                    .on('touchstart.zoom', null)
                    .on('touchmove.zoom', null)
                    .on('touchend.zoom', null);

                var time;
                // Add "background". Otherwise zooming would only be possible with the cursor above nodes/edges.
                svg.append('rect')
                    .call(drag)
                    .attr('width', '100%')
                    .attr('height', '100%')
                    .attr('opacity', '0')
                    .on('mousedown', function(){
                        // On mousedown store the current time in milliseconds
                        time = moment();
                    })
                    .on('mouseup', function(d){
                        // Unselect nodes/edges when clicking on the background without dragging.
                        var now = moment();
                        if(time + 500 > now){
                            unselectNodes();
                            unselectEdges();
                        }
                    });

                link = svg.append('g').selectAll('.link');
                edgepaths = svg.append('g').selectAll(".edgepath");
                edgelabels = svg.append('g').selectAll(".edgelabel")
                node = svg.append('g').selectAll('.node');

            }


            /**
             * gets new graph data using slider values
             * and renders the graph
             */
            function getGraph(){
                $scope.isViewLoading    = true;
                var sliderValue         = [];
                sliderValue.push(parseInt($scope.sliderCountriesCitiesAmount.value));
                sliderValue.push(parseInt($scope.sliderOrganizationsAmount.value));
                sliderValue.push(parseInt($scope.sliderPersonsAmount.value));
                sliderValue.push(parseInt($scope.sliderMiscellaneousAmount.value));
                var sliderEdgeMinFreq   = $scope.sliderEdgeFrequency.value.split(";")[0];
                var sliderEdgeMaxFreq   = $scope.sliderEdgeFrequency.value.split(";")[1];
                var leastOrMostFrequent = Number($scope.freqSorting.least);

                playRoutes.controllers.NetworkController.getGraphData(leastOrMostFrequent, sliderValue, sliderEdgeMinFreq, sliderEdgeMaxFreq).get().then(function(response) {
                    var data = response.data;

                    prepareData(data);

                    calculateNewForceSize();

                    svg.selectAll("*").remove();
                    createSVG();  // Reset svg.
                    nodes.length = 0;
                    edges.length = 0;
                    for (var i = data.nodes.length - 1; i >= 0; i--) {
                        nodes.push(data.nodes[i]);
                    };
                    for (var i = data.links.length - 1; i >= 0; i--) {
                        edges.push(data.links[i]);
                    };

                    start();

                });
            }

            /**
             *	expands the node to every
             *
             *	@param the node to expand
             */
            function expand(node)
            {
            	getEgoNetworkById(node.id, function(){
            		d3.select('#nodebuttonicon_' + node.id).attr('class', 'glyphicon glyphicon-minus');

            		node.expanded = true;
            		console.log(nodes);
            	});
            }

            function collapse(node)
            {

            	edges.forEach(
            		function(v,i,a)
            		{
						if
						(
							(v.target.collapseParent.indexOf(node.id) != -1 && v.target.collapseParent.length == 1) ||
							(v.source.collapseParent.indexOf(node.id) != -1 && v.source.collapseParent.length == 1)
						)
						{
							d3.select("#edgepath_" + v.id).remove();
                            d3.select("#edgeline_" + v.id).remove();
                            d3.select("#edgelabel_" + v.id).remove();
						}
            		}
            	);

				console.log(nodes);
				var deletenodes = [];
            	nodes.forEach(
            		function(v,i,a)
            		{
            			if(v.id == node.id)
            			{
            				return;
            			}

            			console.log(v);
            			if(v.collapseParent.length > 1 && v.collapseParent.indexOf(node.id) != -1)
            			{
            				v.collapseParent.splice(v.collapseParent.indexOf(node.id), 1)
            			}
            			else if(v.collapseParent.length == 1 && v.collapseParent.indexOf(node.id) != -1)
            			{
            				deletenodes.push(v.id);
            				d3.select('#node_' + v.id).remove();
            			}
            		}
            	);

            	nodes = nodes.filter(
            		function(e)
            		{
            			return deletenodes.indexOf(e.id) == -1;
            		}
            	)
            	console.log(nodes);

            	d3.select('#nodebuttonicon_' + node.id).attr('class', 'glyphicon glyphicon-plus');
            	node.expanded = false;


            }

            /**
             * resets the graph and starts the force layout
             */
            function start(callback){
                    var time;

                    // update links
                    link = link.data(force.links(), function(d) { return d.source.id + "-" + d.target.id; });
                    link.enter()
                        .append('line')
                        .attr('id', function(d){return 'edgeline_' + d.id;})
                        .style('opacity', 0)  // Make new edges at first invisible.
                        .style('stroke-width', function (d) {
                            return edgeScale(d.freq)+'px';
                        })
                        .on('mouseup', function (d) {  // when clicking on an edge
                            var index = selectedEdges.indexOf(d);
                            if(index == -1){  // The edge is not selected, so select it.
                                selectedEdges.push(d);
                                d3.select(this).style('stroke', selectionColor)
                                               .style('opacity', .9);
                                d3.select('#edgelabel_' + d.id)
                                            .style('fill', selectionColor)
                                            .attr('font-weight', 'bold');
                            }
                            else{  // The edge is already selected, so unselect it.
                                selectedEdges.splice(index, 1);  // Remove the edge from the list.
                                d3.select(this).style('stroke', '#696969')
                                               .style('obacity', .8);
                                d3.select('#edgelabel_' + d.id)
                                            .style('fill', '#000000')
                                            .attr('font-weight', 'normal');
                            }
                            enableOrDisableButtons();
                        });
                    // remove old links
                    link.exit().remove();

                    // add the new paths for the edge labels
                    edgepaths = edgepaths.data(force.links(), function(d) { return d.source.id + "-" + d.target.id; });
                    edgepaths.enter()
                                .append('path')
                                .attr('class', 'edgepath')
                                .attr('id', function(d, i){
                                    return 'edgepath_' + d.id;
                                })
                                .attr('d', function(d){
                                    return 'M ' + d.source.x + ' ' + d.source.y + ' L ' + d.target.x + ' ' + d.target.y;
                                })
                                .style('pointer-events', 'none');
                    edgepaths.exit().remove();  // Remove old paths.
                    // add the new edge labels
                    edgelabels = edgelabels.data(force.links(), function(d) { return d.source.id + "-" + d.target.id; });
                    edgelabels.enter()
                                .append('text')
                                .attr('id', function(d, i){
                                    return 'edgelabel_' + d.id;
                                })
                                .attr('class', 'edgelabel')
                                .attr('dx', 0)
                                .attr('dy', 15)
                                .attr('font-size', 11)
                                .attr('fill', '#000000')
                                .style('pointer-events', 'none')
                                .append('textPath')
                                .attr('xlink:href',function(d, i){
                                    return '#edgepath_' + d.id;
                                })
                                .style('text-anchor', 'middle')
                                .attr('startOffset', '50%')
                                .text(function(d,i){ return d.freq; })
                                .style('text-shadow', '-1px -1px 3px #FFFFFF, 1px -1px 3px #FFFFFF, -1px 1px 3px #FFFFFF, 1px 1px 3px #FFFFFF');
                    edgelabels.exit().remove();  // Remove old edge labels.

                    // update nodes
                    node = node.data(force.nodes(), function(d) { return d.id;});
                    var newNodes = node.enter()
                                        .append('g')
                                        .attr('id', function(d, i){return 'node_' + d.id})
                                        .attr('class', 'node')
                                        .call(force.drag);

                    newNodes.append('circle')
                            .attr('r', function (d) { return radius(d.freq) })
                            .style('fill', function (d) { return color(d.type); })
                            .style('opacity', 0)  // Make new nodes at first invisible.
                            .attr('id', function(d, i){
                            	return 'nodecircle_' + d.id;
                            })
                            .on('mousedown', function () {  // make nodes clickable
                                // On mousedown store the current time in milliseconds
                                time = moment();
                            })
                            .on('mouseup', function (d) {
                                d3.select(this).classed("fixed", d.fixed = true);  // Fix nodes that where moved.
                                // On mouseup check if it was a click or dragging a circle and in case of click invoke the callback
                                var now = moment();
                                if (time + 500 > now) {
                                    var index = selectedNodes.indexOf(d);
                                    var nodeValue = d.name;
                                    var categoryIndex = $scope.graphShared.getIndexOfCategory(d.type);
                                    if(index == -1){  // The node is not selected, so select it.
                                        selectedNodes.push(d);
                                        d3.select(this)
                                        	.style('stroke-width', 5)
                                            .style('stroke', selectionColor);
                                        // Add the selected node name to the words that get highlighted
                                        // However, add it only once to make deletion of that entry easier
                                        if (highlightShareService.wordsToHighlight[categoryIndex].indexOf(nodeValue) < 0) {
                                            highlightShareService.wordsToHighlight[categoryIndex].push(d.name);
                                            highlightShareService.wasChanged = true;
                                        }
                                    }
                                    else{  // The node is already selected, so unselect it.
                                        selectedNodes.splice(index, 1);  // Remove the node from the list.
                                        d3.select(this).style('stroke-width', 1.5)
                                                       .style('stroke', '#000000');
                                        // Remove the selected node name from the words that get highlighted
                                        var index = highlightShareService.wordsToHighlight[categoryIndex].indexOf(d.name);
                                        if (index > -1) {
                                            highlightShareService.wordsToHighlight[categoryIndex].splice(index, 1);
                                            highlightShareService.wasChanged = true;
                                        }
                                    }
                                    enableOrDisableButtons();
                                }
                            });

                    // set the text so that it fits inside the nodes
                    newNodes.append('text')
                            .attr('dy', '.35em')
                            .attr('text-anchor', 'middle')
                            .attr('id', function(d, i){
                               	return 'nodetext_' + d.id;
                            })
                            .text(function (d){
                                var r = radius(d.freq);
                                if(r/3 >= d.name.length - 1)  // If the text fits inside the node.
                                    return d.name;
                                else  // If the text doesn't fit inside the node the 3 characters "..." are added at the end.
                                    return d.name.substring(0, r/3 - 3) + "...";
                            })
                            .style('font-size', function(d){
                                var r = radius(d.freq);
                                var textLength;
                                var size;
                                if(r/3 >= d.name.length - 1){  // If the text fits inside the node.
                                    textLength = d.name.length;
                                    // If the text contains less than 3 characters: act as if
                                    // there where 3 characters so that the font doesn't become to big.
                                    if(textLength < 3)
                                        textLength = 3;
                                    size = r/3;
                                }
                                else{  // If the text doesn't fit inside the node the 3 characters "..." are added at the end.
                                    textLength = d.name.substring(0, r/3 - 3).length;
                                    size = r/3 - 3;
                                }
                                size *= 7 / textLength;  // This seems to work to get a good text size.
                                size = Math.max(10, Math.round(size));  // Min. text size = 10px
                                return size+'px';
                            });

                    // add buttons to the nodes
                    newNodes.append('foreignObject')
                        .attr('width', '24')
                        .attr('height', '24')
                        .attr('x', -12)
                        .attr('y', function(d){
                            return radius(d.freq) - 2;
                        })
                        .append('xhtml:body')
                        .html(function(d)
                        {
                        	return '<button type="button" id="nodebutton_' + d.id + '" class="btn btn-xs btn-default neighbor-button" ng-show="!isViewLoading"><i id="nodebuttonicon_' + d.id + '" class="glyphicon glyphicon-plus"></i></button>'
                        })
                        .on('click', function(d){
                        	if(d.expanded)
                        	{
                        		collapse(d);
                        	}
                        	else
                        	{
                            	expand(d);
                            }
                        });

                    node.exit().remove();

                    addTooltip(node, link);  // add the tooltips to the nodes and links
                    enableOrDisableButtons();

                    $scope.isViewLoading = false;

                    // use the force L...ayout!
                    force.start();

                    updateWordsForUnderlining();

                    // Set the opacity of all nodes and edges to the normal
                    // values (because the new ones are still invisible).
                    // values (because the new ones are still invisible).
                    // The tansition starts after 2 seconds and takes 3 seconds.
                    setTimeout(function(){
                        d3.selectAll('circle').each(function(d){
                            d3.select(this).transition().duration(3000).style('opacity', 1);
                        });
                        link.each(function(d){
                            d3.select(this).transition().duration(3000).style('opacity', .8);
                        });
                    }, 2000);

					//if we have a callback, we execute it
					if(callback != null)
					{
						callback();
					}
            }


            /**
             * This function adds tooltips to the nodes and links.
             * @param node The nodes
             * @param link The links
             */
            function addTooltip(node, link){
                var tooltip = d3.select("body").append("div").attr("class", "tooltip").style("opacity", 0);

                node.on("mouseover", function(d){
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                    tooltip.transition()
                        .duration(200)
                        .style("opacity", .9);
                    tooltip.html(
                        '<span class="tooltipImportantText">' + d.name +
                            '</span> has the type <span class="tooltipImportantText">'
                            + d.type + '</span> and is <span class="tooltipImportantText">'
                            + d.freq + "</span> times mentioned.")
                        .style("left", (d3.event.pageX - 75) + "px")
                        .style("top", (d3.event.pageY + 25) + "px");
                });
                node.on("mouseout", function(d){
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                });

                link.on("mouseover", function(d){
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                    tooltip.transition()
                        .duration(200)
                        .style("opacity", .9);
                    tooltip.html(
                        '<span class="tooltipImportantText">' + d.source.name +
                            '</span> and <span class="tooltipImportantText">'
                            + d.target.name + '</span> are <span class="tooltipImportantText">'
                            + d.freq + "</span> times mentioned together.")
                        .style("left", (d3.event.pageX - 75) + "px")
                        .style("top", (d3.event.pageY + 25) + "px");
                });
                link.on("mouseout", function(d){
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                });
            }


            /**
             * This function updates the array of the words that will be
             * underlined in documents. The word are underlined in the color
             * the corresponding entity node has.
             */
            function updateWordsForUnderlining(){
                // Remove the old content of the arrays.
                for (var i = 0; i < highlightShareService.wordsToHighlight.length; i++) {
                    highlightShareService.wordsToUnderline[i] = [];
                }
                // Add all node names.
                nodes.forEach(function(node){
                    var categoryIndex = $scope.graphShared.getIndexOfCategory(node.type);
                    // Add every word only once (some nodes have the same name).
                    if(highlightShareService.wordsToUnderline[categoryIndex].indexOf(node.name) == -1){
                        highlightShareService.wordsToUnderline[categoryIndex].push(node.name);
                    }
                });
                highlightShareService.wasChanged = true;
            }


            /**
             * This function disables and enables the buttons for editing
             * the graph depending on the amount of selected nodes/edges.
             */
            function enableOrDisableButtons(){
                // Nothing is selected:
                if(selectedNodes.length == 0 && selectedEdges.length == 0){
                    $('#EgoNetworkButton').attr('disabled', 'disabled');
                    $('#EditButton').attr('disabled', 'disabled');
                    $('#MergeButton').attr('disabled', 'disabled');
                    $('#AnnotateButton').attr('disabled', 'disabled');
                    $('#DeleteButton').attr('disabled', 'disabled');
                    $('#HideButton').attr('disabled', 'disabled');
                }
                // Anything is selected:
                else{
                    $('#DeleteButton').removeAttr("disabled");
                    $('#HideButton').removeAttr("disabled");
                    // Either one node or one edge is selected:
                    if(selectedNodes.length == 1 && selectedEdges.length == 0
                            || (selectedNodes.length == 0 && selectedEdges.length == 1)){
                        $('#MergeButton').attr('disabled', 'disabled');
                        $('#AnnotateButton').removeAttr("disabled");
                        $('#EditButton').removeAttr("disabled");
                        // One node is selected:
                        if(selectedNodes.length == 1)
                            $('#EgoNetworkButton').removeAttr("disabled");
                        // One edge is selected:
                        else
                            $('#EgoNetworkButton').attr('disabled', 'disabled');
                    }
                    // Multiple Nodes and/or edges are selected:
                    else{
                        $('#EgoNetworkButton').attr('disabled', 'disabled');
                        $('#AnnotateButton').attr('disabled', 'disabled');
                        $('#EditButton').attr('disabled', 'disabled');
                        // Only nodes are selected:
                        if(selectedEdges.length == 0)
                            $('#MergeButton').removeAttr("disabled");
                        // Only edges are selected:
                        else
                            $('#MergeButton').attr('disabled', 'disabled');
                    }
                }
            }


            /**
             * this function prepares the data to be read by d3
             * this is done here so the data transmitted by the backend stays relatively small
             * @param  data a data object consisiting of nodes and links
             */
            function prepareData(data){
                // convert each node from array to object
                var nodes = [];
                data.nodes.forEach(function(node) {
                    nodes.push({
                        id: node[0],
                        name: node[1],
                        freq: node[2],
                        type: node[3],
                        size: 2,
                        collapseParent: [],
                        expanded: false
                    });
                });
                data.nodes = nodes;


                // convert each link from array to object
                var links = [];
                data.links.forEach(function(link) {
                    var sourceNode = data.nodes.filter(function(n) { return n.id === link[1]; })[0];
                    var targetNode = data.nodes.filter(function(n) { return n.id === link[2]; })[0];

                    links.push({
                        id: link[0],
                        source: sourceNode,
                        target: targetNode,
                        freq: link[3],
                    });
                });
                data.links = links;

            }


            /** 
             * Create an example graph; we use a trick (namely the $timeout even with 0ms)
             * to render it only after the DOM is fully loaded.
             * Also, initialize the legend popover
             */
            $scope.loaded = function() {
                createSVG();

                force = d3.layout
                    .force()
                    .nodes(nodes)
                    .links(edges)
                    .size([300, 300])
                    .charge(-400)
                    .linkStrength(0.4)
                    .linkDistance(function(d) {
                        return radius(d.source.freq)
                            + radius(d.target.freq)
                            + 100;
                    })
                    .on("tick", function() {
                        link.attr('x1', function (d) {return d.source.x;})
                            .attr('y1', function (d) {return d.source.y;})
                            .attr('x2', function (d) {return d.target.x;})
                            .attr('y2', function (d) {return d.target.y;});
                        node.attr('transform', function (d) {
                            return 'translate(' + d.x + ',' + d.y + ')';
                        });

                        // rotate the edge labels in a way the text isn't upside down
                        edgepaths.attr('d', function(d){
                            return 'M ' + d.source.x + ' ' + d.source.y + ' L ' + d.target.x + ' ' + d.target.y;
                        });
                        edgelabels.attr('transform', function(d, i){
                            if(d.target.x < d.source.x){
                                var x1 = this.__data__.source.x;
                                var x2 = this.__data__.target.x;
                                var y1 = this.__data__.source.y;
                                var y2 = this.__data__.target.y;
                                var x;
                                var y;
                                if(x1 > x2)
                                    x = x1 - (x1 - x2) / 2;
                                else
                                    x = x2 - (x2 - x1) / 2;
                                if(y1 > y2)
                                    y = y1 - (y1 - y2) / 2;
                                else
                                    y = y2 - (y2 - y1) / 2;
                                return 'rotate(180 ' + x + ' ' + y + ')';
                            }
                            else
                                return 'rotate(0)';
                        });
                    });
                getGraph();
                // Extend the popover to a callback when loading is done
                var tmp = $.fn.popover.Constructor.prototype.show;
                $.fn.popover.Constructor.prototype.show = function () {
                    tmp.call(this);
                    if (this.options.callback) {
                        this.options.callback();
                    }
                }
                // On click of the 'Show legend' button show the legend
                $('#btn-show-legend').popover({
                    callback: function() {
                        setLegendPopoverHeight();
                    },
                    placement: 'bottom',
                    html: true,
                    content: function() {
                        return $('#legend-content').html();
                    }
                }).addClass('popover-legend');
            }


            /**
             *  Adds more neighbors of the node with the id 'id'.
             */
            function getMoreNeighbors(id){
                $scope.isViewLoading = true;

                // Get the ids of the nodes that are already in the ego network.
                var existingNodes = [];
                edges.forEach(function(edge){
                    if(edge.source.id == id)
                        existingNodes.push(edge.target.id);
                    if(edge.target.id == id)
                        existingNodes.push(edge.source.id);
                });

                var leastOrMostFrequent = Number($scope.freqSorting.least);
                // Get 4 neighbors (1 of each type).
                playRoutes.controllers.NetworkController.getEgoNetworkData(leastOrMostFrequent, id, [1, 1, 1, 1], existingNodes).get().then(function(response) {
                    var data = response.data;

                    // remove already existing nodes
                    data.nodes = data.nodes.filter(function(n){
                        var arr = nodes.filter(function(m){
                            return m.id == n[0];
                        });
                        return (arr.length == 0);
                    });

                    // remove already existing edges
                    data.links = data.links.filter(function(n){
                        var arr = edges.filter(function(m){
                            return m.id == n[0];
                        });
                        return (arr.length == 0);
                    });


                    force.size([
                        parseInt($('#network-maps-container').css('width')),
                        // -164 because we have to exclude the tab bar and the sliders
                        parseInt($('#network-maps-container').css('height')) - GRAVITATION_HEIGHT_SUBTRACT_VALUE
                    ]);

                    // update node array
                    data.nodes.forEach(function(node) {
                        nodes.push({
                            id: node[0],
                            name: node[1],
                            freq: node[2],
                            type: node[3],
                            size: 2,
                            expanded: false
                        });
                    });

                    // update edge array
                    data.links.forEach(function(link) {
                        var sourceNode = nodes.filter(function(n) { return n.id === link[1]; })[0];
                        var targetNode = nodes.filter(function(n) { return n.id === link[2]; })[0];

                        edges.push({
                            id: link[0],
                            source: sourceNode,
                            target: targetNode,
                            freq: link[3],
                        });
                    });

                    start();
                });
            }



            function getEgoNetworkById(id, callback)
            {
            	var leastOrMostFrequent = Number($scope.freqSorting.least);
                // Get 8 neighbors (2 of each type).
                playRoutes.controllers.NetworkController.getEgoNetworkData(leastOrMostFrequent, id, [2, 2, 2, 2], []).get().then(function(response) {
                var data = response.data;

                // remove already existing nodes
                data.nodes = data.nodes.filter(function(n){
                	var arr = nodes.filter(function(m){
                       	return m.id == n[0];
                    });
                    return (arr.length == 0);
                });
                //data.nodes = data.nodes.filter(function(n){return n.id != id;})

                force.size([
                 	parseInt($('#network-maps-container').css('width')),
                    // -164 because we have to exclude the tab bar and the sliders
                    parseInt($('#network-maps-container').css('height')) - GRAVITATION_HEIGHT_SUBTRACT_VALUE
                ]);

                // update node array
                data.nodes.forEach(function(node)
                {
                	console.log(node);
					nodes.push({
						id: node[0],
						name: node[1],
						freq: node[2],
						type: node[3],
						size: 2,
						collapseParent: [id],
						expanded: false
					});

				});

				// update edge array
				//edges.length = 0;
				data.links.forEach(function(link) {
					var sourceNode = nodes.filter(function(n) { return n.id === link[1]; })[0];
					var targetNode = nodes.filter(function(n) { return n.id === link[2]; })[0];

						edges.push({
							id: link[0],
							source: sourceNode,
							target: targetNode,
							freq: link[3],
						});
				});

				start(callback);
            	});
        	}


            /**
             * Loads the ego network for the selected node.
             */
            $scope.getEgoNetwork = function(){
                if(selectedNodes.length != 0){  // If a node is selected.
                    // Get the id of the newest selected node.
                    var id = selectedNodes[selectedNodes.length-1].id;

                    $scope.isViewLoading = true;

                    var leastOrMostFrequent = Number($scope.freqSorting.least);
                    // Get 8 neighbors (2 of each type).
                    playRoutes.controllers.NetworkController.getEgoNetworkData(leastOrMostFrequent, id, [2, 2, 2, 2], []).get().then(function(response) {
                        var data = response.data;

                        // remove already existing nodes
                        data.nodes = data.nodes.filter(function(n){
                            var arr = nodes.filter(function(m){
                                return m.id == n[0];
                            });
                            return (arr.length == 0);
                        });

                        force.size([
                            parseInt($('#network-maps-container').css('width')),
                            // -164 because we have to exclude the tab bar and the sliders
                            parseInt($('#network-maps-container').css('height')) - GRAVITATION_HEIGHT_SUBTRACT_VALUE
                        ]);

                        // update node array
                        data.nodes.forEach(function(node) {
                            nodes.push({
                                id: node[0],
                                name: node[1],
                                freq: node[2],
                                type: node[3],
                                size: 2,
                                collapseParent: [],
                                expanded: false
                            });
                        });

                        // update edge array
                        edges.length = 0;
                        data.links.forEach(function(link) {
                            var sourceNode = nodes.filter(function(n) { return n.id === link[1]; })[0];
                            var targetNode = nodes.filter(function(n) { return n.id === link[2]; })[0];

                            edges.push({
                                id: link[0],
                                source: sourceNode,
                                target: targetNode,
                                freq: link[3],
                            });
                        });

                        start();
                    });
                }
            }


            /**
             * Hide the currently selected nodes/edges from the graph. The data
             * will still be in the database.
             */
            $scope.hideSelected = function (){
                // Remove selected nodes.
                selectedNodes.forEach(function(d){
                    // Remove the selected node name from the words that get highlighted
                    var categoryIndex = $scope.graphShared.getIndexOfCategory(d.type);
                    var index = highlightShareService.wordsToHighlight[categoryIndex].indexOf(d.name);
                    if(index >= 0){
                        highlightShareService.wordsToHighlight[categoryIndex].splice(index, 1);
                    }
                    nodes.splice(nodes.indexOf(d), 1);  // Remove the node.
                });
                updateWordsForUnderlining();
                highlightShareService.wasChanged = true;

                // Remove selected edges.
                selectedEdges.forEach(function(d){
                    edges.splice(edges.indexOf(d), 1);
                });
                // If the node of an edge was removed: remove the edge.
                for(var i=0; i<edges.length; i++){
                    if(selectedNodes.indexOf(edges[i].source) != -1
                        || selectedNodes.indexOf(edges[i].target) != -1){
                        edges.splice(i, 1);
                        i--;
                    }
                }
                selectedNodes = new Array();
                selectedEdges = new Array();
                enableOrDisableButtons();
                start();
            }


            /**
             * This functions loads a new graph with the opposite frequency ordering.
             */
            $scope.toggleFreqSorting = function () {
                getGraph();
            }

            $scope.editOpen = function()
            {
            	var modal = $uibModal.open(
            		{
            			animation: true,
            			templateUrl: 'editModal',
            			controller: 'EditModalController',
            			size: 'sm',
            			resolve:
            			{
            				text: function(){return $scope.selectedElementsText();},
            				type: function(){return selectedNodes[0].type;},
            				node: function(){return selectedNodes[0];}
            			}
            		}
            	);

            	modal.result.then(function(result)
            	{
					if(result.node.name != result.text)
            		playRoutes.controllers.NetworkController.changeEntityNameById(result.node.id, result.text).get().then(
                    	function(result)
                    	{
                    		if(result.result == false)
                    		{
                    			alert("Error while editing Entity")
                    		}
                    	}
                    );

					if(result.node.type != result.type)
                    playRoutes.controllers.NetworkController.changeEntityTypeById(result.node.id, result.type).get().then(
                       	function(result)
                       	{
                       		if(result.result == false)
                       		{
                       			alert("Error while editing Entity")
                      		}
                       	}
                    );

                    editType(result.node, result.type);
                    editName(result.node, result.text);
            	});
            }

            $scope.annotateOpen = function()
            {
              	var modal = $uibModal.open(
                	{
                		animation: true,
                		templateUrl: 'annotateModal',
                		controller: 'TextModalController',
                		size: 'lg',
                		resolve:
                		{
                			text: function(){return ""},
                			node: function(){return selectedNodes[0];}
                		}
                	}
                );

                modal.result.then(function(result)
                {
                	addAnnotation(result.node, result.text);
                });
            }

            $scope.mergeOpen = function()
            {
            	var modal = $uibModal.open(
                	{
                		animation: true,
                		templateUrl: 'mergeModal',
                		controller: 'MergeModalController',
                		size: 'lg',
                		resolve:
                		{
                			selectedNodes: function(){return selectedNodes;}
                		}
                	}
                );

                modal.result.then(function(result)
                {
                	var focalNode = getNodeById(Number(result.focalNode));
                	merge(focalNode, result.nodes);
                });
            }


            /**
             * Hide the currently selected nodes/edges from the graph. The data
             * will still be in the database.
             */
            $scope.removeSelected = function (){

                //playRoutes.controllers.NetworkController.deleteEntityById(selectedNodes[0].id).get().then(function(result){alert(result)});

				selectedNodes.forEach(
					function(n,i,a){

						d3.select("#node_" + n.id).remove();
						edges.forEach(
							function(v,i,a)
							{
								if(v.source == n || v.target == n)
								{
									d3.select("#edgepath_" + v.id).remove();
									d3.select("#edgeline_" + v.id).remove();
									d3.select("#edgelabel_" + v.id).remove();
								}
							}
						);
					}
				);


                //alert("TODO: delete selected");
            }

            /**
             * merges the currently selected nodes into one
             */
            function merge(focalNode, nodes)
            {
				var entityids = [];
				var freqsum = 0

				//save the ids and delete the merged nodes
				nodes.forEach(
					function(v,i,a)
					{
						if(v.id != focalNode.id)
						{
							entityids.push(v.id);
							d3.select("#node_" + v.id).remove();
							freqsum = freqsum + v.freq;
						}
					}
				);

				//extend the focal node with the sum of the frequencies of
				focalNode.freq = focalNode.freq + freqsum;
				d3.select("#nodecircle_" + focalNode.id).attr('r', function(d){return radius(d.freq);});

				edges.forEach(
					function(v,i,a)
					{
						if(v.target.id == focalNode.id || v.source.id == focalNode.id)
						{
							console.log(v)
						}

						//if we have on both sides a node we want to delete or the focal node
						if(
							(entityids.indexOf(v.source.id) != -1 && entityids.indexOf(v.target.id) != -1) ||
							(v.source.id == focalNode.id && entityids.indexOf(v.target.id) != -1) ||
							(entityids.indexOf(v.source.id) != -1 && v.target.id == focalNode.id)
						)
						{
							//we remove the edge
							d3.select("#edgepath_" + v.id).remove();
                           	d3.select("#edgeline_" + v.id).remove();
                            d3.select("#edgelabel_" + v.id).remove();
                            console.log("both sides")
                            console.log(v)

                            edges.splice(i, 1)
						}
						else if(entityids.indexOf(v.source.id) != -1 || entityids.indexOf(v.target.id) != -1)
						{
							console.log(v)
							//get an existing edge between the focalNode and the node not to delete
							var existingEdge = edges.filter(
								function(e,i,a)
								{
									if(entityids.indexOf(v.source.id) != -1)
									{
										return (e.source.id == v.target.id && e.target.id == focalNode.id) ||
												(e.source.id == focalNode.id && e.target.id == v.target.id)
									}
									else
									{
										return (e.source.id == v.source.id && e.target.id == focalNode.id) ||
                                        		(e.source.id == focalNode.id && e.target.id == v.source.id)
									}
								}
							);

							console.log(existingEdge);

							//if we havent found a node
							if(existingEdge.length == 0)
							{
								//we attach the edge to the focalNode
								if(entityids.indexOf(v.source.id) != -1) v.source = focalNode;
								else v.target = focalNode;
							}
							else
							{
								var edge = existingEdge[0];
								edge.freq = edge.freq + v.freq;

								d3.select("#edgeline_" + edge.id).style('stroke-width', function (d) {return edgeScale(d.freq)+'px';});
								d3.select("#edgelabel_" + edge.id).select("textPath").text(function(d,i){return d.freq;});

								//we remove the edge
                                d3.select("#edgepath_" + v.id).remove();
                                d3.select("#edgeline_" + v.id).remove();
                                d3.select("#edgelabel_" + v.id).remove();

                                edges.splice(i, 1)
							}

						}
					}
				);

				unselectNodes()
				selectNode(focalNode);

				enableOrDisableButtons();
				/*playRoutes.controllers.NetworkController.mergeEntitiesById($scope.merge.node,entityids).get().then(
					function(result)
					{
						alert(result)
					}
				);*/
            }



            /**
             * gives the user the option to annotate selected nodes/edges
             */
            /*$scope.annotateSelected = function (){
                var annotation = $('#annotateInput').val();

                for(var i=0; i<selectedNodes.length; i++)
                {
                	addAnnotation(selectedNodes[i], annotation);
                }

                //alert("TODO: annotate selected --> " + annotation);
            }*/

            /**
             * gives the user the option to edit the name
             * of selected edges
             *
             * @param node the node to modify
             * @param text the new name of the node
             */
            function editName(node, text)
            {
            	//if the name ist the same we return
            	if(node.name == text)
            	{
            		return;
            	}

                var edit = text;
                node.name = edit;
                d3.select("#nodetext_" + node.id).text(function (d){
                	var r = radius(d.freq);
                	if(r/3 >= d.name.length - 1)  // If the text fits inside the node.
                		return d.name;
                	else  // If the text doesn't fit inside the node the 3 characters "..." are added at the end.
                		return d.name.substring(0, r/3 - 3) + "...";
                });
            }

            /**
             *	edit the type of the node
             *
             *	@param node the type of the node
             *	@param type to which the type of the node should be changed
             */
            function editType(node, type)
            {
            	if(node.type == type)
            	{
            		return;
            	}

            	node.type = type;
            	d3.select("#nodecircle_" + node.id)
            		.style("fill", function(d){return color(d.type)});
            }

        }
    ]);


});