/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.controller:FinalJsonCtrl
 * @description #FinalJsonCtrl Final Json Controller of the jsonerApp
 */

'use strict';

angular.module('jsonerApp').controller(
		'FinalJsonCtrl',
		function($scope, $rootScope, jsonBuilderFactory, AdaptorSharedService) {
			$scope.resultedjson = JSON.stringify(jsonBuilderFactory
					.getBuiltAdaptor(), null, 4);

			$scope.init = function() {
				AdaptorSharedService.highlightbreadcrumb();
			};
			$scope.init();
					
				$('#download').click(function(e) {				    
				   this.href = 'data:text/plain;charset=utf-8,'+ encodeURIComponent($scope.resultedjson);
				});
		});