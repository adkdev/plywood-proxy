'use strict';

var gulp = require('gulp');
var watch = require('gulp-watch');
var runSequence = require('run-sequence');

var laborer = require('laborer');

gulp.task('style', laborer.taskStyle());

gulp.task('server:tsc', laborer.taskServerTypeScript({ declaration: true }));

gulp.task('server:test', laborer.taskServerTest());

gulp.task('clean', laborer.taskClean());

gulp.task('all', function(cb) {
  runSequence(
    'clean' ,
    ['style'],
    ['server:tsc'],
    cb
  );
});

gulp.task('all-bundle', function(cb) {
  runSequence(
    'clean' ,
    ['style'],
    ['server:tsc'],
    cb
  );
});

gulp.task('watch', ['all-bundle'], function() {
  watch('./src/client/**/*.scss', function() {
    gulp.start('style');
  });

  watch(['./src/common/**/*.ts', './src/server/**'], function() {
    gulp.start('server:tsc');
  });
});

gulp.task('default', ['all']);
