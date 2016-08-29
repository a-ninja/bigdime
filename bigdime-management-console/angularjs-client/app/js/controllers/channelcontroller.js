/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.controller:ChannelcontrollerCtrl
 * @description # ChannelcontrollerCtrl Controller of the jsonerApp
 */
'use strict';
angular
		.module('jsonerApp')
		.controller(
				'ChannelcontrollerCtrl',
				function($scope, $rootScope, ApplicationService,
						jsonBuilderFactory, HandlerFactory,
						AdaptorSharedService) {

					$scope.channel = {};
					$scope.channel.selected = "";
					var validation;
					var nonessentialfields=AdaptorSharedService.getnonessentialfields();
					$scope.init = function() {
						AdaptorSharedService.highlightbreadcrumb();
						$.each(AdaptorSharedService.getmandatoryfields(),function(index,value){
				        	 $("[id^="+value+"]").addClass('required');
				         });
						$scope.shownext();
					};
					$scope.updateRequiredTag=function(){
						$.each(AdaptorSharedService.getmandatoryfields(),function(index,value){
				        	 $("[id^="+value+"]").addClass('required');
				         });
					};
					$
							.each(
									ApplicationService.getAdaptorConstants(),
									function(index, value) {
										if ($rootScope.environment.selected
												.toLowerCase() === ApplicationService.getAdaptorConstants()[index].environment
												.toLowerCase()) {
											$
													.each(
															ApplicationService.getAdaptorConstants()[index].adaptors,
															function(
																	adaptorindex,
																	value) {
																if ($rootScope.adaptor.selected
																		.toLowerCase() === ApplicationService.getAdaptorConstants()[index].adaptors[adaptorindex].name
																		.toLowerCase()) {
																	$scope.channellist = ApplicationService.getAdaptorConstants()[index].adaptors[adaptorindex].channellist;
																}
															});
										}
									});

					$scope.channels = jsonBuilderFactory.getBuiltAdaptor().channel;

					$scope.isFilled = false;
					$scope.stopPropagation = function() {
						$('.dropdown-menu').click(
								function(e) {
									if (e.target.className.toLowerCase()
											.indexOf("btn-success") >= 0) {
										$scope.ok();
									} else if (e.target.className.toLowerCase()
											.indexOf("btn-danger") >= 0) {
										$scope.cancel();
									} else {
										e.stopPropagation();
									}

								});
					};

					$scope.ok = function() {
						$('.ok').click(function(e) {
							$('.dropdown-toggle').dropdown();
						});
					};
					$scope.reset = function(inputindex) {
						$
								.each(
										ApplicationService.getAdaptorConstants(),
										function(index, value) {
											if ($rootScope.environment.selected
													.toLowerCase() === ApplicationService.getAdaptorConstants()[index].environment
													.toLowerCase()) {
												$
														.each(
																ApplicationService.getAdaptorConstants()[index].adaptors,
																function(
																		adaptorindex,
																		value) {
																	if ($rootScope.adaptor.selected
																			.toLowerCase() === ApplicationService.getAdaptorConstants()[index].adaptors[adaptorindex].name
																			.toLowerCase()) {
																		$
																				.each(
																						ApplicationService.getAdaptorConstants()[index].adaptors[adaptorindex].nondefaults,
																						function(
																								nondefaultindex,
																								value) {
																							if (value
																									.indexOf(".") > -1) {
																								var valueArray = value
																										.split('.');
																								$scope.channels[inputindex][valueArray[0]][valueArray[1]] = "";
																							} else {
																								$scope.channels[inputindex][value] = "";
																							}
																						});
																	}
																});
											}
										});

						$('.reset').on('click', function(event) {
							event.stopPropagation();
						});
						$scope.channels[inputindex] = HandlerFactory.getHandlerObject($scope.channels[inputindex].name);
						$scope.shownext();
					};

					$scope.cancel = function(index) {
						$scope.reset(index);
						$scope.shownext();
					};

					$scope.addchannel = function(channelname) {
						$scope.addnewchannel = HandlerFactory
								.getHandlerObject($scope.channel.selected);
						$scope.addnewchannel['name'] = channelname;
						$scope.channels.push($scope.addnewchannel);
						$scope.channel.selected=null;
						$scope.shownext();
					};

					$scope.removechannel = function(index) {
						$scope.channels.splice(index, 1);
						$scope.shownext();
					};
					
					$scope.shownext = function() {
						validation=true;
						for ( var key in $scope.channels) {
							if (!$scope.iterate($scope.channels, '')) {
								break;
							}
						}
					};

					$scope.iterate = function(obj, stack) {
						$scope.isFilled = false;
						for ( var property in obj) {
							if (obj.hasOwnProperty(property)) {
								if (typeof obj[property] == "object") {
									$scope.iterate(obj[property], stack + '.'
											+ property);
								} else {
									if(validation===true){
										if(nonessentialfields.indexOf(property)==-1){
									if (obj[property] === null
											|| obj[property] === ""
											|| obj[property] === undefined) {
										$scope.isFilled = false;
										validation=false;
										break;
									} else {
										$scope.isFilled = true;
									}
								   }
								  }
								}
							}
						}
					};

					$scope.updateAdaptor = function() {
						for ( var property in jsonBuilderFactory
								.getBuiltAdaptor()) {
							if (jsonBuilderFactory.getBuiltAdaptor()
									.hasOwnProperty(property)) {
								if (typeof jsonBuilderFactory.getBuiltAdaptor()[property] == "object") {
									if (property == 'channel') {
										jsonBuilderFactory.getBuiltAdaptor()[property] = $scope.channels;
									}
								}
							}
						}
						AdaptorSharedService
								.updatebreadcrumbonadadaptorchange('mapper');
					};

					var $thumbs = $('li .breadcrumbtext').click(function(e) {
						e.preventDefault();
						$thumbs.removeClass('highlight');
						$(this).addClass('highlight');
					});
					$scope.init();
				});
