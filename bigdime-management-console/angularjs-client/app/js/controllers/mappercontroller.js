/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.controller:MappercontrollerCtrl
 * @description Controller of the jsonerApp
 */

'use strict';
angular
		.module('jsonerApp')
		.controller(
				'MappercontrollerCtrl',
				function($scope, $rootScope, jsonBuilderFactory,AdaptorSharedService) {
					$scope.sourcelist = [];
					$scope.channellist = [];
					$scope.sinklist = [];
					$scope.isFilled=false;

					$scope.init = function() {
						AdaptorSharedService.highlightbreadcrumb();
						$scope.showNext();
					};

					$scope.initialize = function() {
						for ( var property in jsonBuilderFactory
								.getBuiltAdaptor()) {
							if (jsonBuilderFactory.getBuiltAdaptor()
									.hasOwnProperty(property)) {
								if (typeof jsonBuilderFactory.getBuiltAdaptor()[property] == "object") {
									if (property == 'source') {
										for ( var key in jsonBuilderFactory
												.getBuiltAdaptor()[property]['src-desc']) {
											if (jsonBuilderFactory
													.getBuiltAdaptor()[property]['src-desc']
													.hasOwnProperty(key)) {
												$scope.sourcelist.push(key);
											}
										}

									} else if (property == 'sink') {
										$
												.each(
														jsonBuilderFactory
																.getBuiltAdaptor()[property]['data-handlers'],
														function(index, value) {
															$scope.sinklist
																	.push(jsonBuilderFactory
																			.getBuiltAdaptor()[property]['data-handlers'][index].name);
														});
									} else if (property == 'channel') {
										$
												.each(
														jsonBuilderFactory
																.getBuiltAdaptor().channel,
														function(index, value) {
															$scope.channellist
																	.push(jsonBuilderFactory
																			.getBuiltAdaptor().channel[index].name);
														});
									}
								}
							}
						}
					};
					$scope.initialize();

					$scope.sourcechannelselections = {};
					$scope.channelsinkselections = {};
					$scope.channelselected = "";
					$scope.sinkselected = "";
					var sourceChannelSelection = [];
					var channelSinkSelection = [];

					$scope.uponSourceChannelSelection = function(source,
							channel) {
						var isDuplicate = true;
						if (sourceChannelSelection.length == 0) {
							sourceChannelSelection.push(source + ":" + channel);
						} else {
							$.each(sourceChannelSelection, function(index,
									value) {
								if (sourceChannelSelection[index]
										.indexOf(source) == -1) {
									isDuplicate = false;
								} else if (sourceChannelSelection[index]
										.indexOf(source) >= 0) {
									isDuplicate = true;
									sourceChannelSelection.splice(
											sourceChannelSelection[index]
													.indexOf(source), 1, source
													+ ":" + channel);
									return false;
								}
							});
						}
						if (!isDuplicate) {
							sourceChannelSelection.push(source + ":" + channel);						
						}
						$scope.showNext();
					};
					
					$scope.showNext=function(){
						if(sourceChannelSelection.length==Object.keys(jsonBuilderFactory.getBuiltAdaptor().source["src-desc"]).length){
							$scope.isFilled=true;
						}else{
							$scope.isFilled=false;
						}
					};

					$scope.uponChannelSinkSelection = function(channel, sink) {
						var isDuplicate = true
						if (channelSinkSelection.length == 0) {
							channelSinkSelection.push(channel + ":" + sink);
						} else {
							$.each(channelSinkSelection,
									function(index, value) {
										if (channelSinkSelection[index]
												.indexOf(channel) == -1) {
											isDuplicate = false;
										} else if (channelSinkSelection[index]
												.indexOf(channel) >= 0) {
											isDuplicate = true;
											channelSinkSelection.splice(
													channelSinkSelection[index]
															.indexOf(channel),
													1, channel + ":" + sink);
											return false;
										}
									});
						}
						if (!isDuplicate) {
							channelSinkSelection.push(channel + ":" + sink);
						}
					};

					$scope.updateAdaptor = function() {
						jsonBuilderFactory.getBuiltAdaptor().sink[0]['channel-desc'] = $scope.channellist;
						jsonBuilderFactory.getBuiltAdaptor().source['data-handlers'][jsonBuilderFactory
								.getBuiltAdaptor().source['data-handlers'].length - 1]['properties']['channel-map'] = sourceChannelSelection
								.join();
						AdaptorSharedService
								.updatebreadcrumbonadadaptorchange('result');
					};

					var $thumbs = $('li .breadcrumbtext').click(function(e) {
						e.preventDefault();
						$thumbs.removeClass('highlight');
						$(this).addClass('highlight');
					});
					$scope.init();
				});
