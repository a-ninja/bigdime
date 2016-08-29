/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @description # Defines Grunt Tasks for the application
 */


module.exports = function (grunt) {
    grunt.initConfig({
    	pkg: grunt.file.readJSON('package.json'),
            copy: {
                main: {
                    files: [
//                        {expand: true, src: ['app/**'], dest: '../src/main/webapp/'}
                          {expand: true, cwd:'app/' ,src: ['**'], dest: '../src/main/webapp/'}
                    ]
                }
            },
            watch: {
                scripts: {
                    files: ['app/**'],
                    options: {
                        nospawn: true
                    }
                }
            },

            connect: {
                test: {
                    options: {
                        port: 8000,
                        base: '.'
                    }
                },
                development: {
                    options: {
                        port: 8001,
                        base: '.',
                        keepalive: true
                    }
                }
            },

            karma: {
                files: ['app/js/**/ *.js'],
                unit: {
                    configFile: 'config/karma.conf.js',
                    singleRun: true
                },
                e2e: {
                    configFile: 'config/karma-e2e.conf.js',
                    singleRun: true
                }
            },
            uglify: {
            	options: {
                    banner: '/*! <%= pkg.name %> - v<%= pkg.version %> - ' +'<%= grunt.template.today("yyyy-mm-dd") %> */'
                 },
                first_target: {
                  files:[
                         { src: 'app/js/*js', dest: '../src/main/webapp/app.<%= grunt.template.today(\'yymmddHHMM\') %>.min.js'},
                         { src: 'app/js/controllers/*js', dest: '../src/main/webapp/controllers.<%= grunt.template.today(\'yymmddHHMM\') %>.min.js'},
                         { src: 'app/js/filters/*js', dest: '../src/main/webapp/filters.<%= grunt.template.today(\'yymmddHHMM\') %>.min.js'},
                         { src: 'app/js/services/*js', dest: '../src/main/webapp/services.<%= grunt.template.today(\'yymmddHHMM\') %>.min.js'}
                         ]
                }
              }
        });

    grunt.event.on('watch', function (action, filepath) {
        var destinationPath = "../src/main/webapp/";
        if (action === 'deleted') {
            grunt.log.writeln("WARNING: File " + filepath + " deleted");
        }
        else {
            grunt.log.writeln(filepath + " " + action);
            grunt.file.copy(filepath, destinationPath + filepath);
        }
    });
  
    grunt.registerTask('develop', ['copy', 'watch','uglify']);
    grunt.registerTask('test', ['connect:test', 'karma:unit', 'karma:e2e']);
    grunt.loadNpmTasks('grunt-karma');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-contrib-connect');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-uglify');

}