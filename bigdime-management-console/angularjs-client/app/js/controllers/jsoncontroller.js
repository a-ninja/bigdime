/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.controller:JsoncontrollerCtrl
 * @description
 * # JsoncontrollerCtrl
 * Controller of the jsonerApp
 */

'use strict';
angular.module('jsonerApp')
  .controller('JsoncontrollerCtrl', function ($scope,$rootScope,AdaptorObjectFactory,ApplicationDetailsFactory,jsonBuilderFactory,AdaptorSharedService) {

	 $scope.adaptornames=AdaptorSharedService.getadaptornames();
     $scope.adaptor={};
     $scope.adaptor.selected="";
     $scope.isFilled=false;
     $scope.applicationmetaproperties={};
     var mandatoryfields=[];
     var nonessentialfields=[];
	 
     $scope.init=function(){    
    	 $scope.selectedAdaptorObjectArray=[];	
    	 AdaptorSharedService.highlightbreadcrumb();
 
     };
     $scope.init();
     $scope.uponChange=function(){	
    	  $scope.selectedAdaptorObjectArray=[];	 
    	 if(AdaptorSharedService.adaptor==undefined || AdaptorSharedService.adaptor.selected != $scope.adaptor.selected){
    		 AdaptorSharedService.setadaptorselected($scope.adaptor.selected);
    		 AdaptorSharedService.resetbreadcrumbs(); 
    	 }else{
    		 for(var property in jsonBuilderFactory.getBuiltAdaptor().source["src-desc"]){
    		 if(jsonBuilderFactory.getBuiltAdaptor().source["src-desc"].hasOwnProperty(property)){
    			 AdaptorSharedService.addMore($scope.selectedAdaptorObjectArray,$scope.adaptor.selected);
    			 if($scope.adaptor.selected.toLowerCase()=='kafka'){
    			 $scope.selectedAdaptorObjectArray[$scope.selectedAdaptorObjectArray.length-1]['name']=property;
    			 $scope.selectedAdaptorObjectArray[$scope.selectedAdaptorObjectArray.length-1]['topic']=jsonBuilderFactory.getBuiltAdaptor().source["src-desc"][property]['topic'];
    			 $scope.selectedAdaptorObjectArray[$scope.selectedAdaptorObjectArray.length-1]['partition']=jsonBuilderFactory.getBuiltAdaptor().source["src-desc"][property]['partition'];
    			 $scope.selectedAdaptorObjectArray[$scope.selectedAdaptorObjectArray.length-1]['entity-name']=jsonBuilderFactory.getBuiltAdaptor().source["src-desc"][property]['entity-name'];
    			 $scope.selectedAdaptorObjectArray[$scope.selectedAdaptorObjectArray.length-1]['schemaFileName']=jsonBuilderFactory.getBuiltAdaptor().source["src-desc"][property]['schemaFileName'];
    			 }else if($scope.adaptor.selected.toLowerCase()=='file'){
    				 $scope.selectedAdaptorObjectArray[$scope.selectedAdaptorObjectArray.length-1]['name']=property;
    				 var values=jsonBuilderFactory.getBuiltAdaptor().source["src-desc"][property].split(':');
    				 $scope.selectedAdaptorObjectArray[$scope.selectedAdaptorObjectArray.length-1]['value']=values[0];
    				 $scope.selectedAdaptorObjectArray[$scope.selectedAdaptorObjectArray.length-1]['value2']=values[1];
    			 }else{	  
    			 }
    		 }    		 
    	  }
    	 }
         $.each( mandatoryfields=AdaptorSharedService.getmandatoryfields(),function(index,value){
        	 $('#'+value.toLowerCase()).addClass('required');
         });

    	jsonBuilderFactory.setAdaptorObject($scope.adaptor.selected);
     	for(var property in jsonBuilderFactory.getBuiltAdaptor()){
    		if(jsonBuilderFactory.getBuiltAdaptor().hasOwnProperty(property)){
    			 if(typeof jsonBuilderFactory.getBuiltAdaptor()[property] !="object"){
    				 $scope.applicationmetaproperties[property]=jsonBuilderFactory.getBuiltAdaptor()[property];
    			 }
    		}
    	} 
     	   nonessentialfields=AdaptorSharedService.getnonessentialfields()
	      $rootScope.adaptor=$scope.adaptor; 
    	  $scope.addMore();
    };

    $scope.addMore =function(){  
        AdaptorSharedService.addMore($scope.selectedAdaptorObjectArray,$scope.adaptor.selected);
        $scope.showNext();
  };

  $scope.remove=function(index){
  	AdaptorSharedService.remove(index,$scope.selectedAdaptorObjectArray);
      $scope.showNext();
  };

    $scope.showNext=function(){
      outer: for(var i=0;i<$scope.selectedAdaptorObjectArray.length;i++){
        inner: for(var key in $scope.selectedAdaptorObjectArray[i] ){
          if($scope.selectedAdaptorObjectArray[i][key] !=="" && $scope.selectedAdaptorObjectArray[i][key]!==null && $scope.selectedAdaptorObjectArray[i][key]!==undefined){
             for(var keys in $scope.applicationmetaproperties){
            	if(mandatoryfields.indexOf(keys)>=0){
            	if($scope.applicationmetaproperties[keys]==null || $scope.applicationmetaproperties[keys]==""||$scope.applicationmetaproperties[keys]==undefined){
            		$scope.isFilled = false;
            		break outer;
            	}else{
        	        $scope.isFilled = true;
            	}
             }
            	
            }

          } else if(nonessentialfields.indexOf(key)==-1){
            $scope.isFilled = false;
            break outer;
          }
          
        }
      }
    };
    
      $scope.updateAdaptor=function(){
    	  AdaptorSharedService.updateAdaptor('adaptor','source','',$scope.applicationmetaproperties,$scope.selectedAdaptorObjectArray,'source', $scope.adaptor.selected.toLowerCase());
      };
  });
