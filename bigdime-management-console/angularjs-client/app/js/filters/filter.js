
/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @description # Provides filters required in the application
 */

angular.module('jsonerApp')
.filter('formatDate',function(){
	return function(inputLongFormatdate){
		var dateArray=new Date(inputLongFormatdate).toGMTString().split(" ");
		return dateArray[0]+" "+dateArray[2]+" "+dateArray[1]+" "+dateArray[4]+" UTC "+dateArray[3];
	};
});