/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @description # Provides factory implementations required by the application
 */

/**
 * @ngdoc service
 * @name jsonerApp.jsonFactory
 * @description
 * # jsonFactory
 * Factory in the jsonerApp.
 */

'use strict';
angular.module('jsonerApp')
 .factory('AdaptorObjectFactory',function(){
  return{
    getSelectionObject:function(applicationname){
    	if(applicationname.toLowerCase()==='kafka'){
      return {
    	name:"",
        topic:"",
        partition:"",
        'entity-name':"",
        schemaFileName:""
        
      };
    	}else if(applicationname.toLowerCase()==='sql'){
    		return{
    			name:"",
    			query:"",
    			incrementby:"",
    			targettable:"",
    			partitioncoulmns:""
    
    		};
    	}else if(applicationname.toLowerCase()=='file'){
    		 return {
    		        name:"",
    		        value:"",
    		        value2:""
    		      };
    	}
    }
  };
});

angular.module('jsonerApp')
.factory('HandlerFactory',function($rootScope,ApplicationService){
  return{
	  getHandlerObject:function(handlername) {
		  var dataReturned;
		  var url;
		  $.each(ApplicationService.getEnvConstants(),function(index,value){
				if(ApplicationService.getEnvConstants()[index].environment.toLowerCase()===$rootScope.environment.selected.toLowerCase()){
					 url=ApplicationService.getEnvConstants()[index].url+ApplicationService.getEnvConstants()[index].port+"/"+ApplicationService.getEnvConstants()[index].application+'/rest/alertService/data-platform/bigdime-adaptor/handlerTemplate?handlerId=handler.'+handlername.toLowerCase();
				}
			});
		  $.ajax({
			  dataType: "json",
			  url:url,
			  async: false,
			  success: function(data) { 
				  dataReturned=data;
				}
		});
		 return dataReturned;
	  }
    };
 });

angular.module('jsonerApp')
.factory('sqlSelectorFactory',function(){
  return{
    getSqlObject:function(){
      return{
        name:"",
        query:"",
        incrementedBy:"",
        targetTableName:"",
        partitionedColumns:""
      };
    }
  };
});

angular.module('jsonerApp')
 .factory('fileSelectorFactory',function(){

  return{
    getFileObject:function(){
      return{
        name:"",
        value:""
      };
    }
  };
});

angular.module('jsonerApp')
.factory('ApplicationDetailsFactory',function(){
	 return{
		    getApplicationDetailsObject:function(){
		     	return{
		     		name:"",
			        type:"",
			        cronexpression:"",
			        autostart:"",
			        namespace:"",
			        description:""
		     	};
		    }
	};
});

angular.module('jsonerApp')
 .factory('jsonBuilderFactory',function($rootScope,ApplicationService){
	 var adaptorObject={};
	 var adaptorSelected="";
	 return{
     setAdaptorObject:function(applicationname){  
	  var url="";
	  $.each(ApplicationService.getEnvConstants(),function(index,value){
			if(ApplicationService.getEnvConstants()[index].environment.toLowerCase()===$rootScope.environment.selected.toLowerCase()){
				 url=ApplicationService.getEnvConstants()[index].url+ApplicationService.getEnvConstants()[index].port+"/"+ApplicationService.getEnvConstants()[index].application+'/rest/alertService/data-platform/bigdime-adaptor/configTemplate?templateId=admin.'+applicationname.toLowerCase()
			}
		});
	  if(Object.getOwnPropertyNames(adaptorObject).length == 0 || adaptorSelected.toLowerCase()!=applicationname.toLowerCase() ){
		  $.ajax({
			  dataType: "json",
			  url:url,
			  async: false,
			  success: function(data) { 
				  adaptorObject=data;
				  adaptorSelected=applicationname;
				}
		});		 
	  }
	  return adaptorObject;
  },
  getBuiltAdaptor:function(){
	  return adaptorObject;
  },
  resetBuiltAdaptor:function(){
	   adaptorObject={};
  }
  
 }
});





