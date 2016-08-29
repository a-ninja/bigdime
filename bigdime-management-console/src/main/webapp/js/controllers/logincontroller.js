/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.controller:LogincontrollerCtrl
 * @description
 * #LogincontrollerCtrl
 * Login Controller of the jsonerApp
 */

'use strict';

angular.module('jsonerApp')
.controller('LogincontrollerCtrl', function ($scope,$location, AuthenticationService) {
	$scope.vm = this;
	$scope.vm.login=function(){
		 $scope.vm.dataLoading = true;
         AuthenticationService.Login($scope.vm.username, $scope.vm.password, function (response) {
        	 
             if (response.success.loginStatus==true) {
            	 $('.invalid').hide();
                 AuthenticationService.SetCredentials($scope.vm.username, $scope.vm.password,response.success.loginStatus,response.success.displayName);
                 $location.path('/homepage');
             } else {
                 $scope.vm.dataLoading = false;
            	 $('.invalid').show();
            	 $location.path('/indexpage');
             }
         });
	};
});
