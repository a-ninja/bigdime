/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.AuthenticationService
 * @description # Provides authentication specific services
 */

'use strict';
angular.module('jsonerApp').factory('AuthenticationService',
		function($http, $rootScope) {

			var service = {};
			service.Login = Login;
			service.SetCredentials = SetCredentials;
			return service;
			function Login(username, password, callback) {
				$http.post('/bigdime-management-console/rest/service/authenticate', {
					"username" : username,
					"password" : password
				}).success(function(response) {
					callback({
						success : response
					});
				});
			}

			function SetCredentials(username, password,loginStatus,displayName) {
				$rootScope.globals = {
					currentUser : {
						username : username,
						password : password,
						loginStatus:loginStatus,
						displayName:displayName
					}
				};
			}

		});

angular.module('jsonerApp').factory('applicationFactory', function($rootScope) {
	return {
		getApplicationObject : function() {
			return {
				"name" : "",
				"datesArray" : []
			};
		}
	};
});



angular.module('jsonerApp').factory('SearchService',function($http,$rootScope){
	 
});



