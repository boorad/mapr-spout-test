var g = g || {};

g.Cloud = function(){
    return {
        url : "data/users.json",
        $j : jQuery,

        users : [],
        tags : [],
        fontsize : null,

        svg : null,
        vis : null,
        background : null,

        //defaults
        width           : 400,
        height          : 520,
        totalValue      : 0,
        max             : 200,
        changed         : false,

        fill : d3.scale.category20b(),

        init : function() {
            var that = this;

            that.svg = d3.select("#users").selectAll("svg").data([0]);
            that.svg.enter().append("svg")
                .attr("width", that.width);

            that.background = that.svg.append("g");

            that.vis = that.svg.append("g")
                .attr("transform", "translate("
                      + [that.width >> 1, that.height >> 1] + ")");

            that.getData(function() { that.generate() });

        },

        progress : function(d) {},

        getData : function(callback) {
            var that = this;

            $.ajax({
                url : that.url,
                dataType : "json",
                cache : false,
                success : function(json, status, jqXHR) {
                    that.data = json;
                    that.processData();
                    that.parseText();
                    callback();
                },
                error : function(jqXHR, status, error) {
                    that.data = {};
                    callback();
                }
            });
        },

        processData : function() {
            var that = this;
            var users = [];
            $.each( that.data, function(user, cnt) {
                for( var i=0; i < cnt; i++ ) {
                    users.push(user);
                }
            });
            that.users = users.join(" ");
        },

        parseText : function() {
            var that = this;
            that.tags = [];
            var cases = {};
            var separators = /[\s\u3031-\u3035\u309b\u309c\u30a0\u30fc\uff70]+/g;
            that.users.split(separators).forEach(function(word) {
                //if (discard.test(word)) return;
                //word = word.replace(punctuation, "");
                //if (stopWords.test(word.toLowerCase())) return;
                //if (word.length <= 3) return;
                //word = word.substr(0, maxLength);
                cases[word] = word;
                that.tags[word] = (that.tags[word] || 0) + 1;
            });
            that.tags = d3.entries(that.tags).sort(function(a, b) {
                return b.value - a.value;
            });
            that.tags.forEach(function(d) { d.key = cases[d.key]; });
        },

        generate : function() {
            var that = this;

            that.layout = d3.layout.cloud()
                .font("Impact")
                .spiral("archimedean")
                .timeInterval(10)
                .size([that.width, that.height])
                .fontSize(function(d) { return that.fontSize(+d.value); })
                .text(function(d) { return d.key; })
                .rotate(function(d,i) { return i & 1 ? 90 : 0; })
                .on("word", that.progress)
                .on("end", that.render);

            that.fontSize = d3.scale["log"]().range([10, 100]);
            if( that.tags.length ) {
                that.fontSize.domain([+that.tags[that.tags.length - 1].value
                                      || 1, +that.tags[0].value]);
                that.changed = true;
                that.update();
            }
        },

        update : function() {
            var that = this;

            if( that.changed ) {
                that.layout
                    .stop()
                    .words(that.tags)
                    .start();
                changed = false;
            }
        },

        render : function(data, bounds) {
            var that = g.u;
            var w = that.width, h = that.height;

            var scale = bounds ? Math.min(
                w / Math.abs(bounds[1].x - w / 2),
                w / Math.abs(bounds[0].x - w / 2),
                h / Math.abs(bounds[1].y - h / 2),
                h / Math.abs(bounds[0].y - h / 2)) / 2 : 1;

            var text = that.vis.selectAll("text")
                .data(data, function(d) {
                    return d.text;
                });

            text.transition()
                .duration(1000)
                .attr("transform", function(d) { return "translate(" + [d.x, d.y]
                                                 + ")rotate(" + d.rotate + ")"; })
                .style("font-size", function(d) { return d.size + "px"; });
            text.enter().append("text")
                .attr("text-anchor", "middle")
                .attr("transform", function(d) { return "translate(" + [d.x, d.y]
                                                 + ")rotate(" + d.rotate + ")"; })
                .style("font-size", function(d) { return d.size + "px"; })
                .style("opacity", 1e-6)
                .style("stroke-width", 1)
                .style("stroke", function(d) {return that.fill(d.text); })
                .on("mouseover", function(d,i) {
                    var el = d3.select(this);
                    el.style("stroke","#fff");
                })
                .on("mouseout", function(d,i) {
                    var el = d3.select(this);
                    el.style("stroke", function(d) {return that.fill(d.text); })
                })
                .transition()
                .duration(1000)
                .style("opacity", 1);
            text.style("font-family", function(d) { return d.font; })
                .style("fill", function(d) { return that.fill(d.text); })
                .text(function(d) { return d.text; });
            var exitGroup = that.background.append("g")
                .attr("transform", that.vis.attr("transform"));
            var exitGroupNode = exitGroup.node();
            text.exit().each(function() {
                exitGroupNode.appendChild(this);
            });
            exitGroup.transition()
                .duration(1000)
                .style("opacity", 1e-6)
                .remove();

        }

    }

}

/*


var fill = d3.scale.category20b();

var w = 410,
    h = 600;

var words = [],
max = 100,
scale = 1,
complete = 0,
keyword = "",
tags,
fontSize,
maxLength = 30,
//fetcher = "http://search.twitter.com/search.json?rpp=100&q=marketing",
url = "http://m.hb.dev:1080/heartbyteapp/libraries/widgets/buzzwords/word/4fa57644825759c638000006/all",
statusText = d3.select("#status"),
curr_doc_id = -1,
changed = false;

var layout = d3.layout.cloud()
    .timeInterval(10)
    .size([w, h])
    .fontSize(function(d) { return fontSize(+d.value); })
    .text(function(d) { return d.key; })
    .rotate(function(d,i) { return i & 1 ? 90 : 0; })
    .on("word", progress)
    .on("end", draw);

var svg = d3.select("#cloud").append("svg")
    .attr("width", w)
    .attr("height", h);

var background = svg.append("g"),
vis = svg.append("g")
    .attr("transform", "translate(" + [w >> 1, h >> 1] + ")");

var getJSON = jsonp(),
paletteJSON = jsonp().callback("jsonCallback");

function draw(data, bounds) {
    scale = bounds ? Math.min(
        w / Math.abs(bounds[1].x - w / 2),
        w / Math.abs(bounds[0].x - w / 2),
        h / Math.abs(bounds[1].y - h / 2),
        h / Math.abs(bounds[0].y - h / 2)) / 2 : 1;
    words = data;

    var text = vis.selectAll("text")
        .data(words, function(d) { return d.text.toLowerCase(); });
    text.transition()
        .duration(1000)
        .attr("transform", function(d) { return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")"; })
        .style("font-size", function(d) { return d.size + "px"; });
    text.enter().append("text")
        .attr("text-anchor", "middle")
        .attr("transform", function(d) { return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")"; })
        .style("font-size", function(d) { return d.size + "px"; })
//        .on("click", function(d) {
//            load(d.text);
//        })
        .style("opacity", 1e-6)
        .transition()
        .duration(1000)
        .style("opacity", 1);
    text.style("font-family", function(d) { return d.font; })
        .style("fill", function(d) { return fill(d.text.toLowerCase()); })
        .text(function(d) { return d.text; });
    var exitGroup = background.append("g")
        .attr("transform", vis.attr("transform"));
    var exitGroupNode = exitGroup.node();
    text.exit().each(function() {
        exitGroupNode.appendChild(this);
    });
    exitGroup.transition()
        .duration(1000)
        .style("opacity", 1e-6)
        .remove();
    vis.transition()
        .delay(1000)
        .duration(750)
        .attr("transform", "translate(" + [w >> 1, h >> 1] + ")scale(" + scale + ")");
}

function getData(f) {
    $.ajax({
        url : url,
        dataType : "json",
        success : function(d) {
            console.log("success", d);
            if( d.id != curr_doc_id ) {
                changed = true;
                curr_doc_id = d.id;
            }
            words = expand_words(d.words);
            parseText(words);
            f();
        },
        error : function(jqXHR) {
            console.log("error", jqXHR);
        }
    });
}

function expand_words(wrds) {
    var expanded = [];
    for( wrd in seed ) {
        if( seed.hasOwnProperty(wrd) ) {
            //for( var j=0; j < seed[wrd]; j++ ) {
                expanded.push(wrd);
            //}
        }
    }
    for( wrd in wrds ) {
        if( wrds.hasOwnProperty(wrd) ) {
            for( var j=0; j < wrds[wrd]; j++ ) {
                expanded.push(wrd);
            }
        }
    }
    return expanded.join(" ");
}

// From Jonathan Feinberg's cue.language, see lib/cue.language/license.txt.
var stopWords = /^(i|me|my|myself|we|us|our|ours|ourselves|you|your|yours|yourself|yourselves|he|him|his|himself|she|her|hers|herself|it|its|itself|they|them|their|theirs|themselves|what|which|who|whom|whose|this|that|these|those|am|is|are|was|were|be|been|being|have|has|had|having|do|does|did|doing|will|would|should|can|could|ought|i'm|you're|he's|she's|it's|we're|they're|i've|you've|we've|they've|i'd|you'd|he'd|she'd|we'd|they'd|i'll|you'll|he'll|she'll|we'll|they'll|isn't|aren't|wasn't|weren't|hasn't|haven't|hadn't|doesn't|don't|didn't|won't|wouldn't|shan't|shouldn't|can't|cannot|couldn't|mustn't|let's|that's|who's|what's|here's|there's|when's|where's|why's|how's|a|an|the|and|but|if|or|because|as|until|while|of|at|by|for|with|about|against|between|into|through|during|before|after|above|below|to|from|up|upon|down|in|out|on|off|over|under|again|further|then|once|here|there|when|where|why|how|all|any|both|each|few|more|most|other|some|such|no|nor|not|only|own|same|so|than|too|very|say|says|said|shall)$/,
punctuation = /[!"&()*+,-\.\/:;<=>?\[\\\]^`\{|\}~]+/g,
wordSeparators = /[\s\u3031-\u3035\u309b\u309c\u30a0\u30fc\uff70]+/g,
discard = /^(@|https?:)/,
htmlTags = /(<[^>]*?>|<script.*?<\/script>|<style.*?<\/style>|<head.*?><\/head>)/g,
matchTwitter = /^https?:\/\/([^\.]*\.)?twitter\.com/;

function parseText(text) {
    console.log("parseText", text);
    tags = [];
    var cases = {};
    text.split(wordSeparators).forEach(function(word) {
        if (discard.test(word)) return;
        word = word.replace(punctuation, "");
        if (stopWords.test(word.toLowerCase())) return;
        if (word.length <= 3) return;
        word = word.substr(0, maxLength);
        cases[word.toLowerCase()] = word;
        tags[word = word.toLowerCase()] = (tags[word] || 0) + 1;
    });
    tags = d3.entries(tags).sort(function(a, b) { return b.value - a.value; });
    tags.forEach(function(d) { d.key = cases[d.key]; });
}

function generate() {
    layout
        .font("Impact")
        .spiral("archimedean");
    fontSize = d3.scale["log"]().range([10, 100]);
    if (tags.length)
        fontSize.domain([+tags[tags.length - 1].value || 1, +tags[0].value]);
    complete = 0;
    words = [];
    layout.stop().words(tags.slice(0, max = Math.min(tags.length, max))).start();
}

function progress(d) {}

function load(d, f) {
    getData(generate);
}

function update() {
    if( changed ) {
        layout.stop().words(tags.slice(0, max = Math.min(tags.length, max))).start();
        changed = false;
    }
}


d3.select("#random-palette").on("click", function() {
    paletteJSON("http://www.colourlovers.com/api/palettes/random", {}, function(d) {
        fill.range(d[0].colors);
        vis.selectAll("text")
            .style("fill", function(d) { return fill(d.text.toLowerCase()); });
    });
    d3.event.preventDefault();
});


load();

// refresh data loop
var refresh_data = window.setInterval( function() {
    getData(update);
}, 2000 );

*/
