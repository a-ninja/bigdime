/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @description # Provides application constants and routes for the application
 */
'use strict';

/**
 * @ngdoc overview
 * @name jsonerApp
 * @description
 * # jsonerApp
 *
 * Main module of the application.
 */
angular
.module('jsonerApp', ['ui.router'])
.config(function ($stateProvider, $urlRouterProvider) {
	$urlRouterProvider.otherwise('/indexpage');
	
	$stateProvider
	 .state('indexpage', {
         url: '/indexpage',
         templateUrl: 'views/index/login.html',
         controller: 'LogincontrollerCtrl'
     })
    .state('homepage', {
         url: '/homepage',
         views:{
        	 '':{templateUrl:'views/home/homepage.html',
                 controller:'HomecontrollerCtrl'
        		 },
        	 'applicationtree@homepage':{
        		 templateUrl:'views/home/hometree.html',
                 controller:'HomecontrollerCtrl'
                },
        	 'applicationheader@homepage':{
        		 templateUrl:'views/home/applicationheader.html',
                 controller:'HomecontrollerCtrl'
        	 }
    
         }
     })
     .state('homepage.search',{
    	 url: '/search',
         templateUrl: 'views/search/homesearch.html',
         controller: 'SearchcontrollerCtrl'
     })
     .state('homepage.jsonhome',{
    	 url: '/jsonhome',
         templateUrl: 'views/adaptorsselection/selectortable.html',
         controller: 'JsoncontrollerCtrl'
     })
	 .state('homepage.sourcehandlers', {
         url: '/sourcehandlers',
         templateUrl: 'views/sourcehandlerselection/sourcehandlers.html',
         controller: 'SourcehandlercontrollerCtrl'
     })
      .state('homepage.sinkhandlers', {
         url: '/sinkhandlers',
         templateUrl: 'views/sinkhandlerselection/sinkhandlers.html',
         controller: 'SinkhandlercontrollerCtrl'
     })
     .state('homepage.channelselector', {
         url: '/channelselector',
         templateUrl: 'views/channelselection/channelselector.html',
         controller: 'ChannelcontrollerCtrl'
     })
     .state('homepage.sourcechannelsinkmapper', {
         url: '/sourcechannelsinkmapper',
         templateUrl: 'views/sourcechannelsinkselection/sourcechannelsinkmapper.html',
         controller: 'MappercontrollerCtrl'
     })
     .state('homepage.viewjson', {
         url: '/viewjson',
         templateUrl: 'views/sourcechannelsinkselection/viewjson.html',
         controller: 'FinalJsonCtrl'
     });
});