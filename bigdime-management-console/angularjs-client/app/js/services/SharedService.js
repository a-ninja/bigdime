/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.SharedService
 * @description # Provides Shared services which will be used by multiple controllers
 */

angular.module('jsonerApp').factory('SharedService',function($rootScope,$timeout,$http,ApplicationService){
	var sharedService={};
	  sharedService.applicationselected="";
      sharedService.adaptorList=[];
      sharedService.getjqxTree = getjqxTree;
      sharedService.updateRoot=updateRoot;
	  sharedService.prepareBroadCast=function(applicationname){
		  this.applicationselected=applicationname;
		  sharedService.broadcastchange();  
	  };
	  var applicationSelected="";
	  var environmentSelected="";
	  sharedService.broadcastchange=function(){
		  $timeout(function(){
		  $rootScope.$broadcast('handleApplicationchangedBroadcast',sharedService.applicationselected);
	       },1000);
	  };
	  
	  sharedService.highlightbreadcrumb=function(){
			var $thumbs = $('li .breadcrumbtext').click(function(e) {
			    e.preventDefault();
			    $thumbs.removeClass('highlight');
			    $(this).addClass('highlight');
			});
	  };
	  
	  sharedService.updatebreadcrumbonadadaptorchange=function(){		  
		  $rootScope.breadcrumbs={
		    		 "adaptor":"true",
		    		 "source":"false",
		    		 "sink":"false",
		    		 "channel":"false",
		    		 "mapping":"false",
		    		 "result":"false"
		     };		  
      };
	  
      sharedService.getadaptorlist=function(){
   	   $.each(ApplicationService.getAdaptorConstants(),function(index,value){
			 if($rootScope.environment.selected.toLowerCase()===ApplicationService.getAdaptorConstants()[index].environment.toLowerCase()){
				 adaptorList= ApplicationService.getAdaptorConstants()[index].adaptors;
			 }
		 });
      };
      
      sharedService.gethandlertList=function(){
   	   $.each(ApplicationService.getAdaptorConstants(),function(index,value){
   		 if($rootScope.environment.selected.toLowerCase()===ApplicationService.getAdaptorConstants()[index].environment.toLowerCase()){
   			 $.each(ApplicationService.getAdaptorConstants()[index].adaptors,function(adaptorindex,value){	
   				 if($rootScope.adaptor.selected.toLowerCase()===ApplicationService.getAdaptorConstants()[index].adaptors[adaptorindex].name.toLowerCase()){
   			          $scope.handlerlist= ApplicationService.getAdaptorConstants()[index].adaptors[adaptorindex].handlerlist;	
   				 }
   			 });
   		   }
   	        });
      };
		function getjqxTree(environment,callback) {				
			$.each(ApplicationService.getEnvConstants(),function(index,value){
				if(ApplicationService.getEnvConstants()[index].environment.toLowerCase()===environment.toLowerCase()){
					$http.get(ApplicationService.getEnvConstants()[index].url+ApplicationService.getEnvConstants()[index].port+"/"+ApplicationService.getEnvConstants()[index].application+'/rest/alertService/alertset').success(function(response){
						$rootScope.applicationlist = response;
						if((applicationSelected=="" && environmentSelected=="")||(environmentSelected!==$rootScope.environment.selected)){
							applicationSelected=response[0].label;
							environmentSelected=$rootScope.environment.selected;
						    sharedService.prepareBroadCast(response[0].label);
						}
						callback(response);
					});
				}
			});				
			
		}
		function updateRoot(applicationname) {
			    sharedService.prepareBroadCast(applicationname);	
		};
	  
	   return sharedService; 
});