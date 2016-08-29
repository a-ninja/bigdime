/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.controller:HomecontrollerCtrl
 * @description #HomecontrollerCtrl Home Controller of the jsonerApp
 */

'use strict';

angular.module('jsonerApp').controller(
		'HomecontrollerCtrl',
		function($scope, $location, $rootScope, $filter, ApplicationService,SharedService,AdaptorSharedService,jsonBuilderFactory) {
			$scope.environmentoptions = [ 'Dev', 'Qa', 'Prod' ];
			$scope.environment = {};
			$scope.environment.selected = $scope.environmentoptions[0];
			$scope.userid = $rootScope.globals.currentUser.username;
			$scope.displayName = $rootScope.globals.currentUser.displayName;			
			 $scope.init = function() {
				//get the applications of the left hand panel
				$scope.getjqxTree();
				//get the adaptor constants
				ApplicationService.setAdaptorConstants($rootScope.environment.selected );
				
			};
			$scope.getjqxTree = function() {
				$rootScope.environment = $scope.environment;
			     SharedService.getjqxTree($scope.environment.selected,
						function(response) {
							$scope.applicationlist = response;
						});
			};			

			$scope.updateRoot = function(applicationname) {
				SharedService.updateRoot(applicationname);	
			};
			$scope.logout =function(){
				AdaptorSharedService.resetbreadcrumbs();
				jsonBuilderFactory.resetBuiltAdaptor();
			};

			var $thumbs = $('li .breadcrumbtext').click(function(e) {
				e.preventDefault();
				$thumbs.removeClass('highlight');
				$(this).addClass('highlight');
			});

		});