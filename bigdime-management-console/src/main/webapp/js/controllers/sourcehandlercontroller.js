/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.controller:SourcehandlercontrollerCtrl
 * @description # SourcehandlercontrollerCtrl Controller of the jsonerApp
 */
'use strict';
angular
		.module('jsonerApp')
		.controller(
				'SourcehandlercontrollerCtrl',
				function($scope, $rootScope, ApplicationService,
						jsonBuilderFactory,
						HandlerFactory, AdaptorSharedService) {
					$scope.handler = {};
					$scope.handler.selected = "";
					$scope.handlernames = AdaptorSharedService.handlernames();
					$scope.handlerclassnames=AdaptorSharedService.handlerclassnames('source');
					$scope.datahandlers = jsonBuilderFactory.getBuiltAdaptor().source['data-handlers'];
					$scope.source = jsonBuilderFactory.getBuiltAdaptor().source;
					$scope.defaulthandlercount=AdaptorSharedService.defaulthandlercount('source');
					$scope.isFilled = false;
					var validation;
					var mandatoryfields = AdaptorSharedService
							.getmandatoryfields();
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
																								$scope.datahandlers[inputindex][valueArray[0]][valueArray[1]] = "";
																							} else {
																								$scope.datahandlers[inputindex][value] = "";
																							}

																						});
																	}
																});
											}
										});

						$('.reset').on('click', function(event) {
							event.stopPropagation();
						});
						$scope.datahandlers[inputindex] = HandlerFactory.getHandlerObject($scope.datahandlers[inputindex].name);
						$scope.shownext();
					};

					$scope.cancel = function(index) {
						$scope.reset(index);
						$scope.shownext();
					};

					$scope.addsourcehandler = function(handlername) {
						$scope.addhandler = HandlerFactory
								.getHandlerObject($scope.handler.selected);
						$scope.addhandler['name'] = handlername;
						$scope.datahandlers.push($scope.addhandler);
						$scope.shownext();
					};

					$scope.addproperty = function(index, key, value) {
						$scope.datahandlers[index][key] = value;
					};

					$scope.removesourcehandler = function(index) {
						$scope.datahandlers.splice(index, 1);
						$scope.shownext();
					};

					$scope.shownext = function() {
						validation=true;
						for ( var key in $scope.source) {
							if (!$scope.iterate($scope.source, '')) {
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
						AdaptorSharedService.updateAdaptor('source', 'sink',
								$scope.datahandlers);
					};
					$scope.init();
				});
