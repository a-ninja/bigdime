/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @name jsonerApp.AdaptorSharedService
 * @description # Provides adaptor specific services
 */

angular.module('jsonerApp').factory('AdaptorSharedService',function($rootScope,$timeout,ApplicationService,AdaptorObjectFactory,jsonBuilderFactory){
	var adaptorSharedService={};
	adaptorSharedService.adaptor={};
	  $rootScope.breadcrumbs={
	    		 "adaptor":"true",
	    		 "source":"false",
	    		 "sink":"false",
	    		 "channel":"false",
	    		 "mapping":"false",
	    		 "result":"false"
	     };	
	adaptorSharedService.highlightbreadcrumb=function(){
			var $thumbs = $('li .breadcrumbtext').click(function(e) {
			    e.preventDefault();
			    $thumbs.removeClass('highlight');
			    $(this).addClass('highlight');
			});
	  };
	  
	  adaptorSharedService.updatebreadcrumbonadadaptorchange=function(entity){		  
		  $rootScope.breadcrumbs[entity]="true";
      };
      
      adaptorSharedService.setselection=function(entity){
    	  if(entity='adaptor'){
    		  
    	  }    	  
      };
      
      adaptorSharedService.resetbreadcrumbs=function(){
    	  $rootScope.breadcrumbs={
 	    		 "adaptor":"true",
 	    		 "source":"false",
 	    		 "sink":"false",
 	    		 "channel":"false",
 	    		 "mapping":"false",
 	    		 "result":"false"
 	     };	
      };
      
      adaptorSharedService.getHandlerTypesSelected=function(datahadlers){
    	  var list=[];
    	  $.each(datahandlers,function(index,value){
    		  datahandlers[index].name.push(list);
    	  });
    	  return list;
      };
      
      adaptorSharedService.getlist=function(){
       var list=[];
   	   $.each(ApplicationService.getAdaptorConstants(),function(index,value){
			 if($rootScope.environment.selected.toLowerCase()===ApplicationService.getAdaptorConstants()[index].environment.toLowerCase()){
				 $.each(ApplicationService.getAdaptorConstants()[index].adaptors,function(adaptorindex,value){
					 list.push(ApplicationService.getAdaptorConstants()[index].adaptors[adaptorindex]);
				 });
			 }
		 });
   	   return list;
      };
      
      adaptorSharedService.getadaptors=function(){
    	  var list=[];
    	$.each( adaptors=adaptorSharedService.getlist(),function(index,value){
    		list.push(adaptors[index]);
    	});
    	return list;
      };
      
      adaptorSharedService.getadaptornames=function(){
    	  var list=[];
    	$.each( adaptor=adaptorSharedService.getadaptors(),function(index,value){
    		list.push(adaptor[index].name);
    	});
    	return list;
      };
      
      adaptorSharedService.getmandatoryfields=function(){
    	  var list=[];
    		$.each(adaptor=adaptorSharedService.getadaptors(),function(index,value){
    	  if(adaptorSharedService.adaptor.selected.toLowerCase()===adaptor[index].name.toLowerCase()){
    		   list=adaptor[index].mandatoryfields;
	         }         
    		});
    		return list;
       };
       
       adaptorSharedService.getnonessentialfields=function(){
     	  var list=[];
     		$.each(adaptor=adaptorSharedService.getadaptors(),function(index,value){
     	  if(adaptorSharedService.adaptor.selected.toLowerCase()===adaptor[index].name.toLowerCase()){
     		   list=adaptor[index].nonessentialfields;
 	         }         
     		});
     		return list;
        };
        
        adaptorSharedService.gethtmlfields=function(entity){
       	  var list=[];
       		$.each(adaptor=adaptorSharedService.getadaptors(),function(index,value){
       	  if(adaptorSharedService.adaptor.selected.toLowerCase()===adaptor[index].name.toLowerCase()){
       		   list=adaptor[index].loadhtmlfields[entity];
   	         }         
       		});
       		return list;
          };
          
      adaptorSharedService.setadaptorselected=function(adaptorselected){
    	  adaptorSharedService.adaptor.selected=adaptorselected;
      };
      
      adaptorSharedService.addMore=function(selectedAdaptorObjectArray,adaptorselected){
    	  
    	  var adaptorObject=AdaptorObjectFactory.getSelectionObject(adaptorselected)
    	  selectedAdaptorObjectArray.push(AdaptorObjectFactory.getSelectionObject(adaptorselected));
    	  
      };
      
      adaptorSharedService.remove=function(index,selectedAdaptorObjectArray){
          selectedAdaptorObjectArray.splice(index,1);
      };
      adaptorSharedService.handlernames=function(){
    	  var list=[];
  		$.each(adaptor=adaptorSharedService.getadaptors(),function(index,value){
  	  if(adaptorSharedService.adaptor.selected.toLowerCase()===adaptor[index].name.toLowerCase()){
  		   list=adaptor[index].handlerlist;
	         }         
  		});
  		return list;
      };
      adaptorSharedService.handlerclassnames=function(entity){
    	  var list=[];
  		$.each(adaptor=adaptorSharedService.getadaptors(),function(index,value){
  	  if(adaptorSharedService.adaptor.selected.toLowerCase()===adaptor[index].name.toLowerCase()){
  		  if(entity==='source'){
  		   list=adaptor[index].handlerclasslist;
  		  }else if(entity==='sink'){
  			list=adaptor[index].sinkhandlerclasslist;  
  		  }
	         }         
  		});
  		return list;
      };
      adaptorSharedService.sinkhandlernames=function(){
    	  var list=[];
  		$.each(adaptor=adaptorSharedService.getadaptors(),function(index,value){
  	  if(adaptorSharedService.adaptor.selected.toLowerCase()===adaptor[index].name.toLowerCase()){
  		   list=adaptor[index].sinkhandlerlist;
	         }         
  		});
  		return list;
      };
      adaptorSharedService.defaulthandlercount=function(entity){
    	  if(entity==='source'){
          return jsonBuilderFactory.getBuiltAdaptor()[entity]['data-handlers'].length;
    	  }else{
    		  return jsonBuilderFactory.getBuiltAdaptor()[entity][0]['data-handlers'].length; 
    	  }
      };
      
      adaptorSharedService.addhandler=function(handlerselected,handlervalue){         
         var handler=HandlerFactory.getHandlerObject(handlerselected);
         handler['name']=handlervalue;
         return handler;
     
      };
      
      adaptorSharedService.shownext=function(entity){
    	  var isFilled=true;	
    	  for (var key in entity){
    		  var result=iterate(entity);
    	    	if(!result){
    	    	   isFilled=false;
    	           break;
    	      }
    	    }
    	  return isFilled;
      };
      
      var iterate=function(obj){
    	  var isFilled=false;
          for(var property in obj){
            	 isFilled=false;
             if(obj.hasOwnProperty(property)){
                 if(typeof obj[property]=="object" ){
	                  if($.isEmptyObject(obj[property])){
	                	  isFilled=false;
	                	  return isFilled;
	                	  }else{
                	         iterate(obj[property]);
	                	  }
                 }else{
                   if(obj[property]===null || obj[property]==="" || obj[property] ===undefined){
                      isFilled=false; 
                     return isFilled;
                   }else{
                    isFilled=true;
                   }
                 }
             }
          }
         return isFilled; 
      };
      
        adaptorSharedService.updateAdaptor=function(currententity,nextentity,datahandlers,applicationmetaproperties,selectedAdaptorObjectArray,source,adaptor){ 
    	  for(var property in jsonBuilderFactory.getBuiltAdaptor()){
      		if(jsonBuilderFactory.getBuiltAdaptor().hasOwnProperty(property)){
      			 if(typeof jsonBuilderFactory.getBuiltAdaptor()[property] =="object"){
      				 if(property==currententity){
      				 jsonBuilderFactory.getBuiltAdaptor()[property]['data-handlers']=datahandlers;     			 
      				 }else if (property==source){   					 
      					var dupes = {};
  						var singles = [];      						
  						$.each(selectedAdaptorObjectArray, function(index, obj) {
  						    if (!dupes[obj.name]) {
  						        dupes[obj.name] = true;
  						        singles.push(obj);
  						    }
  						});
  						
      					 if(adaptor =='kafka'){
       						    						 
      						$.each(singles, function(index, value) {
      							var name=value['name'];
      							delete  value['name'];
      							jsonBuilderFactory.getBuiltAdaptor().source['src-desc'][name]=value;   						   
      				     });
      					 }else if(adaptor =='file'){
      						$.each(selectedAdaptorObjectArray,function(index,value){
                                jsonBuilderFactory.getBuiltAdaptor().source["src-desc"][selectedAdaptorObjectArray[index].name]=selectedAdaptorObjectArray[index].value+':'+selectedAdaptorObjectArray[index].value2;
            				 });
      					 }else if(adaptor =='sql'){
      						$.each(singles, function(index, value) {
      							var name=value['name'];
      							delete  value['name'];
      							jsonBuilderFactory.getBuiltAdaptor().source['src-desc'][name]=adaptorSharedService.sqlGenerator(value);						   
      				     });
      					 }
      				 }
      				console.log(JSON.stringify(jsonBuilderFactory.getBuiltAdaptor(),null,4));
      			   }else{
      				   if(currententity=="adaptor"){
      				        jsonBuilderFactory.getBuiltAdaptor()[property]=applicationmetaproperties[property];
      				   }
      			   }
      		}
      	 } 
      	adaptorSharedService.updatebreadcrumbonadadaptorchange(nextentity); 
      };
      
      adaptorSharedService.sqlGenerator=function(value){
            return'"{\"query\":\"'+value.query+ '\", \"incrementedBy\": \"'+value.incrementby+ '\", \"partitionedColumns\": \"'+value.partitioncoulmns+'\"} "';    	
      };
      
      adaptorSharedService.reset=function(inputindex){
    	  $.each(ApplicationService.getAdaptorConstants(),function(index,value){
    	  if($rootScope.environment.selected.toLowerCase()===ApplicationService.getAdaptorConstants()[index].environment.toLowerCase()){
    		  $.each(defaults=ApplicationService.getAdaptorConstants()[index].adaptors,function(adaptorindex,value){	
    			  for(var property in nondefault){
    				  if(typeof nondefault[property] =="object"){
    					  
    				  }
    			  }
    		  });
    	    } 
    	  });
      };
      
      
	   return adaptorSharedService; 
});