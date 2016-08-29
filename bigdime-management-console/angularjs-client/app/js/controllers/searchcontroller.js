/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.controller:ChannelcontrollerCtrl
 * @description
 * # ChannelcontrollerCtrl
 * Controller of the jsonerApp
 */
'use strict';
angular
		.module('jsonerApp')
		.controller(
				'SearchcontrollerCtrl',
				function($scope, $rootScope, $filter, ApplicationService,
						SharedService) {
					// init
					$scope.search={};
					var sortingOrder = "";
					$scope.sortingOrder = sortingOrder;
					$scope.reverse = false;
					$scope.filteredItems = [];
					$scope.groupedItems = [];
					$scope.itemsPerPage = 10;
					$scope.pagedItems = [];
					$scope.currentPage = 0;
					$scope.search.query = "";
					$scope.items = [];
					$scope.applicatioName="";
					$scope.dataLoad={};

					var searchMatch = function(haystack, needle) {
						if (!needle) {
							return true;
						}
						return haystack.toLowerCase().indexOf(
								needle.toLowerCase()) !== -1;
					};

					// init the filtered items
					$scope.search = function() {
						$scope.filteredItems = $filter('filter')(
								$scope.items,
								function(item) {
									for ( var attr in item) {
										if ($.inArray(attr, [
												'applicationName', 'logLevel','dateTimeString',
												'severity', 'messageContext',
												'message' ]) > -1) {
											if (searchMatch(item[attr],
													$scope.search.query))
												return true;
										}
									}
									return false;
								});
						// take care of the sorting order
						if ($scope.sortingOrder !== '') {
							$scope.filteredItems = $filter('orderBy')(
									$scope.filteredItems, $scope.sortingOrder,
									$scope.reverse);
						}
						if($scope.search.query !=="" && $scope.search.query !==undefined){
						$scope.currentPage = 0;
						}
						// now group by pages
						$scope.groupToPages();
					};

					// calculate page in place
					$scope.groupToPages = function() {
						$scope.pagedItems = [];

						for (var i = 0; i < $scope.filteredItems.length; i++) {
							if (i % $scope.itemsPerPage === 0) {
								$scope.pagedItems[Math.floor(i
										/ $scope.itemsPerPage)] = [ $scope.filteredItems[i] ];
							} else {
								$scope.pagedItems[Math.floor(i
										/ $scope.itemsPerPage)]
										.push($scope.filteredItems[i]);
							}
						}
					};

					$scope.range = function(start, end) {
						var ret = [];
						if (!end) {
							end = start;
							start = 0;
						}
						for (var i = start; i < end; i++) {
							ret.push(i);
						}
						return ret;
					};

					$scope.prevPage = function() {
						if ($scope.currentPage > 0) {
							$scope.currentPage--;
						}
					};

					$scope.nextPage = function() {
						if ($scope.currentPage < $scope.pagedItems.length - 1) {
							$scope.currentPage++;
						}
						if($scope.currentPage >=$scope.pagedItems.length-1){
							$scope.dataLoad.dataLoading=true;
							$scope.items.sort(function(a, b){return b.dateTime-a.dateTime});
							getAlertData(SharedService.applicationselected,$scope.items[$scope.items.length-1]['dateTime']+1 );
						}
					};

					$scope.setPage = function() {
						$scope.currentPage = this.n;
						if($scope.currentPage >=$scope.pagedItems.length-1){
							$scope.dataLoad.dataLoading=true;
							$scope.items.sort(function(a, b){return b.dateTime-a.dateTime});
							getAlertData(SharedService.applicationselected,$scope.items[$scope.items.length-1]['dateTime']+1 );
						}
					};

					// change sorting order
					$scope.sort_by = function(newSortingOrder) {
						if ($scope.sortingOrder == newSortingOrder)
							$scope.reverse = !$scope.reverse;
						    $scope.sortingOrder = newSortingOrder;

					};

					$scope.$on('handleApplicationchangedBroadcast', function(event,args) {
						$scope.pagedItems = [];
						$scope.items = [];
						$scope.filteredItems = [];
						$scope.currentPage = 0;
						$scope.dataLoad.dataLoading=true;
						$scope.dataLoad.isEmpty=false;
						$scope.applicatioName=SharedService.applicationselected;
						getAlertData(SharedService.applicationselected,
								new Date().getTime());
					});

					var getAlertData = function(applicationname, offsetdate) {
						ApplicationService.getAlertData(applicationname,
								offsetdate, function(response) {
							           $.each(response.raisedAlerts,function(index,value){
							        	   response.raisedAlerts[index]['dateTimeString']=$filter('formatDate')(response.raisedAlerts[index]['dateTime']);
							           });
							    	   $scope.items=$scope.items.concat(response.raisedAlerts);
									$scope.search();
									$scope.dataLoad.dataLoading=false;
									if($scope.items.length==0){
										$scope.dataLoad.isEmpty=true;
										$scope.dataLoad.itemCount=0;
									}else{
										$scope.dataLoad.isEmpty=false;
										$scope.dataLoad.itemCount=$scope.items.length;
									}
								});
						
					};

				});
