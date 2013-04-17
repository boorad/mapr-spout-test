// Create a function that returns a particular property of its parameter.
// If that property is a function, invoke it (and pass optional params).
function ƒ(name){
  var v,params=Array.prototype.slice.call(arguments,1);
  return function(o){
    return (typeof (v=o[name])==='function' ? v.apply(o,params) : v );
  };
}

// Return the first argument passed in
function I(d){ return d }

var g = g || {};

g.Chart = function(){
    return {
        $j : jQuery,
        delay : 5000,

        url : "data/tweets.json",
        query_url : "data/query.json",
        query : "{unknown}",
        hide_top : 2,
        MAX_HIDE : 25,

        //defaults
        width           : 550,
        height          : 550,
        groupPadding    : 10,
        totalValue      : 0,


        // will be calculated later
        maxRadius       : null,
        centerX         : null,
        centerY         : null,
        scatterPlotY    : null,

        //d3 settings
        defaultGravity  : 0.1,
        defaultCharge   : function(d){
            if (d.value < 0) {
                return 0
            } else {
                return -Math.pow(d.radius,2.0)/8
            };
        },
        links           : [],
        nodes           : [],
        positiveNodes   : [],
        force           : {},
        svg             : {},
        circle          : {},
        gravity         : null,
        charge          : null,
        changeTickValues: [-0.25, -0.15, -0.05, 0.05, 0.15, 0.25],
        categorizeChange: function(c){
            if (isNaN(c)) {
                return 0;
            } else if ( c < -0.25) {
                return -3;
            } else if ( c < -0.05){
                return -2;
            } else if ( c < -0.001){
                return -1;
            } else if ( c <= 0.05){
                return 1;
            } else if ( c <= 0.25){
                return 2;
            } else {
                return 3;
            }
        },
        fillColor       : "#fff",
        strokeColor     : "#666",
        getFillColor    : null,
        getStrokeColor  : null,
        pFormat         : d3.format("+.1%"),
        pctFormat       : function(){return false},
        tickChangeFormat: d3.format("+%"),
        simpleFormat    : d3.format(","),
        simpleDecimal   : d3.format(",.2f"),

        nameFormat      : function(n){return n},

        rScale          : null,
        radiusScale     : null,
        changeScale     : d3.scale.linear().domain([-0.28,0.28]).range([620,180]).clamp(true),
        sizeScale       : d3.scale.linear().domain([0,110]).range([0,1]),
        groupScale      : {},

        //data settings
        currentYearDataColumn   : 'cnt',
        data                    : g.tweet_data,
        categoryPositionLookup  : {},
        categoriesList          : [],

        //
        //
        //
        init: function() {
            var that = this;

            $(".settings #hide_top").data("old", that.hide_top);
            that.bindSettingsControls();

            this.scatterPlotY = this.changeScale(0);

            this.pctFormat = function(p){
                if (p === Infinity ||p === -Infinity) {
                    return "N.A."
                } else {
                    return that.pFormat(p)
                }
            }

            this.getStrokeColor = function(d){
                return that.strokeColor(d.changeCategory);
            };
            this.getFillColor = function(d){
                if (d.isNegative) {
                    return "#fff"
                }
                return that.fillColor(d.changeCategory);
            };

            this.centerX = this.width / 2;
            this.centerY = 250;

            var svg = d3.select("#words").selectAll("svg").data([0]);
            svg.enter().append("svg")
                .attr("width", this.width);
            this.svg = svg;

            that.getQueryData();

            that.getWordData(function() {
                that.render();
                that.start();
                that.totalLayout();

                that.data_loop();
            });
        },

        bindSettingsControls : function() {
            var that = this;

            // settings dropdown has complex fields, so stop click from closing it
            $(".settings-menu").click(function(e) {
                e.stopPropagation();
            });

            // new query 'go' button clicked
            $("#new_query").click(function(e) {
                var q = $("#query_input").val();
                that.updateQuery(q);
                that.setQuery(q);
            });

            // up and down buttons for hiding top N words
            $(".settings .up").click(function(e) {
                that.changeHideTop(1, true);
            });

            $(".settings .down").click(function(e) {
                that.changeHideTop(-1, true);
            });

            $(".settings #hide_top").change(function(e) {
                var diff = parseInt($(this).val()) - $(this).data("old");
                that.changeHideTop(diff, false);
            });
        },

        changeHideTop : function(incr, update) {
            var that = this;
            var n = $(".settings #hide_top").val();
            n = parseInt(n) || 0;
            if( update ) n += incr;
            if( n < 0 ) n = 0;
            var max = Math.min(that.adata.length, that.MAX_HIDE);
            if( n > max ) n = max;
            $(".settings #hide_top").val(n);
            $(".settings #hide_top").data("old", n);
            that.hide_top = n;
        },

        updateQuery : function(q) {
            $.ajax({
                type : "POST",
                url : "/newquery",
                data : "q=" + q,
                success : function() {},
                dataType : "text"
            });
        },

        getWordData : function(callback) {
            var that = this;
            $.ajax({
                url : that.url,
                dataType : "json",
                cache : false,
                success : function(json, status, jqXHR) {
                    that.data = json;
                    var changed = that.processData();
                    if( changed ) callback();
                },
                error : function(jqXHR, status, error) {
                    that.data = {};
                    callback();
                }
            });
        },

        getQueryData : function() {
            var that = this;
            $.ajax({
                url : that.query_url,
                dataType : "json",
                cache : false,
                success : function(json, status, jqXHR) {
                    if( json.q ) {
                        that.setQuery(json.q);
                    } else {
                        that.setQuery("{unknown}");
                    }
                },
                error : function(jqXHR, status, error) {
                    console.error(jqXHR, status, error);
                    that.setQuery("{unknown}");
                }
            });
        },

        setQuery : function(q) {
            var that = this;
            that.query = q;
            $("#query_input").val(q);
            $("#words .query").html(q);
        },

        processData : function() {
            var changed = false;
            var that = this;
            that.totalValue = 0;

            // get totals first - double traversal, sigh.
            for( i in that.data ) {
                if( that.data.hasOwnProperty(i) ) {
                    that.totalValue = that.totalValue + that.data[i];
                }
            }
            that.rScale = d3.scale.pow().exponent(0.5)
                .domain([0,that.totalValue/5]).range([1,90]);
            that.radiusScale = function(n){ return that.rScale(Math.abs(n)); };

            // Builds the nodes data array from the original data
            if( !that.data ) return true;
            var data = that.clone(that.data); // local copy

            // turn data into an array of objects
            var adata = $.map(data, function(val, key) {
                return {"word" : key, "count" : val};
            });

            // sort it (ascending) and hide top N (
            adata = adata.sort(function(o1,o2) {
                return o1.count-o2.count;
            });
            that.adata = adata; // save this for calc in changeHideTop
            adata = adata.slice(0, adata.length -
                        Math.min(that.hide_top, adata.length));

            // hack - put adata back into data
            var newdata = {};
            $.each(adata, function(i, e) {
                newdata[e.word] = e.count;
            });
            data = newdata;

            // first, traverse existing nodes
            for(var i=0; i<that.nodes.length; i++) {
                if( !that.nodes[i] ) continue;
                var o = that.nodes[i];

                if( data[o.word] ) { // in both
                    if( parseInt(data[o.word]) != parseInt(o.cnt) ) {
                        // new value
                        //console.log("changed", o.word, o.cnt, data[o.word]);
                        that.nodes[i].cnt = data[o.word];
                        changed = true;
                    }
                    that.nodes[i].radius = that.radiusScale(data[o.word]);
                    delete data[o.word]
                } else { // in nodes but not in data
                    //console.log("removed", o.word, o.cnt);
                    that.nodes.splice(i,1);
                    changed = true;
                }
            }

            // now go thru anything remaining in data and add to nodes
            $.each( data, function(word, cnt) {
                //console.log("added", word, cnt);
                that.nodes.push({
                    "word"   : word,
                    "cnt"    : cnt,
                    "radius" : that.radiusScale(cnt)
                });
                changed = true;
            });

            return changed;
        },

        data_loop : function() {
            var that = this;
            this.data_interval = setInterval(function() {
                // make sure force-directed layout is done before starting loop
                if( that.force.alpha() < 0.01 ) {
                    that.force.stop();
                    that.getWordData(function() {
                        that.render();
                        that.force
                            .on("tick", function(e){
                                that.tick(e, 0);
                            })
                            .start();
                    });
                }
            }, that.delay);
        },

        pause_data_loop : function() {
            clearInterval(this.data_interval);
        },

        render : function() {
            var that = this;

            var lft = that.svg[0][0].offsetLeft;
            var top = that.svg[0][0].offsetTop;

            // circles per word
            var word = this.svg.selectAll(".word")
                .data(that.nodes, ƒ('word'));
            var g = word.enter().append("g")
                .attr("class","word")

            var circle = g.append("circle")
                .style("fill", that.fillColor)
                .style("stroke-width", 1)
                .style("stroke", that.strokeColor)
                .on("mouseover",function(d,i) {
                    var el = d3.select(this)
                    var xpos = Number(el.attr('cx')) + lft
                    var ypos = Number(el.attr('cy')) - d.radius - 10 + top
                    el.style("stroke","#000").style("stroke-width",3);
                    d3.select("#tooltip")
                        .style('top',ypos+"px")
                        .style('left',xpos+"px")
                        .style('display','block')
                    d3.select("#tooltip .name").
                        html(that.nameFormat(d.word))
                    d3.select("#tooltip .value")
                        .html(that.formatNumber(d.cnt))
                })
                .on("mouseout",function(d,i) {
                    d3.select(this)
                        .style("stroke-width",1)
                        .style("stroke", that.strokeColor)
                    d3.select("#tooltip")
                        .style('display','none')});

            word.transition().duration(200).select("circle")
                .attr("r", ƒ('radius'));

            //word.transition().duration(200).select("text")

            word.exit().transition().duration(300).remove();

            var text = g.append("text")
                .attr("text-anchor", "middle")
                .attr("dx", ƒ('x'))
                .attr("dy", ".35em")
                .attr("display", function(d) {
                    if( (d.radius / d.word.length) > 3)
                        return "inline"
                    else
                        return "none";
                })
                .text(ƒ('word'));

            that.word = word;
        },

        loop : function() {
            this.data_loop();
        },

        pause : function() {
            this.pause_data_loop();
        },

        //
        //
        //
        start: function() {
            var that = this;
            this.force = d3.layout.force()
                .nodes(that.nodes)
                .size([that.width, that.height]);
        },

        //
        //
        //
        totalLayout: function() {
            var that = this;
            this.force
                .gravity(-0.01)
                .charge(that.defaultCharge)
                .friction(0.9)
                .on("tick", function(e){
                    that.tick(e, e.alpha);
                })
                .start();
        },

        tick : function(e, buoyancy) {
            var that = this;
            that.word.select("circle")
                .each(that.totalSort(e.alpha))
                .each(that.buoyancy(buoyancy))
                .attr("cx", ƒ('x'))
                .attr("cy", ƒ('y'));
            that.word.select("text")
                .attr("transform", function(d) {
                    return "translate(" + d.x + "," + d.y + ")";
                });
        },

        // --------------------------------------------------------------------
        // FORCES
        // --------------------------------------------------------------------


        //
        //
        //
        totalSort: function(alpha) {
            var that = this;
            return function(d){
                var targetY = that.centerY;
                var targetX = that.width / 2;

                d.y = d.y + (targetY-d.y) * (that.defaultGravity + 0.02) * alpha
                d.x = d.x + (targetX-d.x) * (that.defaultGravity + 0.02) * alpha

            };
        },

        //
        //
        //
        buoyancy: function(alpha) {
            var that = this;
            return function(d){
                var targetY = that.centerY;
                d.y = d.y + (targetY - d.y) * (that.defaultGravity) * alpha *
                    alpha * alpha * 100
            };
        },

        formatNumber : function(n,decimals) {
            var s, remainder, num, negativePrefix, negativeSuffix, prefix, suffix;
            suffix = ""
            negativePrefix = ""
            negativeSuffix = ""
            if (n < 0) {
                negativePrefix = "";
                negativeSuffix = " in income"
                n = -n
            };

            if (n >= 1000000000000) {
                suffix = " trillion"
                n = n / 1000000000000
                decimals = 2
            } else if (n >= 1000000000) {
                suffix = " billion"
                n = n / 1000000000
                decimals = 1
            } else if (n >= 1000000) {
                suffix = " million"
                n = n / 1000000
                decimals = 1
            }


            prefix = ""
            if (decimals > 0) {
                if (n<1) {prefix = "0"};
                s = String(Math.round(n * (Math.pow(10,decimals))));
                if (s < 10) {
                    remainder = "0" + s.substr(s.length-(decimals),decimals);
                    num = "";
                } else{
                    remainder = s.substr(s.length-(decimals),decimals);
                    num = s.substr(0,s.length - decimals);
                }


                return  negativePrefix + prefix + num.replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,") + "." + remainder + suffix + negativeSuffix;
            } else {
                s = String(Math.round(n));
                s = s.replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
                return  negativePrefix + s + suffix + negativeSuffix;
            }
        },

        clone : function(obj) {
            return jQuery.extend(true, {}, obj);
        }
    }
};

g.ready = function() {
    // bubble chart
    g.c = new g.Chart();
    g.c.init();
    // username cloud
    g.u = new g.Cloud();
    g.u.init();
}

if (!!document.createElementNS && !!document.createElementNS('http://www.w3.org/2000/svg', "svg").createSVGRect){
    $(document).ready($.proxy(g.ready, this));
} else {
    $("#container").hide();
}
