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
        url : "data/tweets.json",
        $j : jQuery,
        //defaults
        width           : 550,
        height          : 850,
        groupPadding    : 10,
        totalValue      : 0,


        //will be calculated later
//        boundingRadius  : null,
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
            this.centerY = 300;

            var svg = d3.select("#words").selectAll("svg").data([0]);
            svg.enter().append("svg")
                .attr("width", this.width);
            this.svg = svg;

            this.getData();

        },

        getData : function() {
            var that = this;
            d3.json(that.url, function(error, json) {
                if( error ) return console.warn(error);
                var firstp = (!that.data || that.data.length == 0);
                that.data = json;
                that.processData();
                that.render();
                if( firstp ) {
                    that.start();
                    that.totalLayout();
                }
                //that.dev(2);
            });
        },

        processData : function() {
            var that = this;
            that.totalValue = 0;

            // get totals first - double traversal, sigh.
            for (var i=0; i < this.data.length; i++) {
                var n = this.data[i];
                that.totalValue += n[this.currentYearDataColumn];
            }
            that.rScale = d3.scale.pow().exponent(0.5)
                .domain([0,that.totalValue/6]).range([1,90]);
            that.radiusScale = function(n){ return that.rScale(Math.abs(n)); };

            // Builds the nodes data array from the original data
            for (var i=0; i < this.data.length; i++) {
                var n = this.data[i];
                var out = {
                    id: i,
                    radius: this.radiusScale(n[this.currentYearDataColumn]),
                    value: n[this.currentYearDataColumn],
                    name: n['word'],
                    isNegative: (n[this.currentYearDataColumn] < 0),
                }

                that.nodes[i] = out;
            };


        },

        // for dev only
        dev : function(wait) {
            var that = this;
            console.log(g.c.nodes[0]);
            setTimeout(function() {
                that.nodes[0].value = 103;
                that.nodes[0].radius = that.radiusScale(that.nodes[0].value);
                var circle = that.svg.selectAll("circle")
                    .data(that.nodes, ƒ('id'))
                circle.transition(200).call(that.circleAttrs);
                that.totalLayout();
                console.log(g.c.nodes[0]);
            }, wait * 1000);

        },

        render : function() {
            var that = this;

            var circle = this.svg.selectAll("circle").data(that.nodes, ƒ('id'));

            var lft = that.svg[0][0].offsetLeft;
            var top = that.svg[0][0].offsetTop;


            circle.enter().append("circle")
                .style("fill", that.fillColor)
                .style("stroke-width", 1)
                .style("stroke", that.strokeColor)
                .call(that.circleAttrs)
                .on("mouseover",function(d,i) {
                    var el = d3.select(this)
                    var xpos = Number(el.attr('cx')) + lft
                    var ypos = (el.attr('cy') - d.radius - 10 + top)
                    el.style("stroke","#000").style("stroke-width",3);
                    d3.select("#tooltip")
                        .style('top',ypos+"px")
                        .style('left',xpos+"px")
                        .style('display','block')
                    d3.select("#tooltip .name").
                        html(that.nameFormat(d.name))
                    d3.select("#tooltip .value")
                        .html(that.formatNumber(d.value))
/*
                    d3.select("#g-tooltip .g-discretion")
                        .text(that.discretionFormat(d.discretion))
                    d3.select("#g-tooltip .g-department").text(d.group)

                    var pctchngout = that.pctFormat(d.change)
                    if (d.change == "N.A.") {
                        pctchngout = "N.A."
                    };
                    d3.select("#g-tooltip .g-change").html(pctchngout)
*/
                })
                .on("mouseout",function(d,i) {
                    d3.select(this)
                        .style("stroke-width",1)
                        .style("stroke", that.strokeColor)
                    d3.select("#tooltip")
                        .style('display','none')});

            circle.exit().transition().duration(300).remove();
            circle.call(that.circleAttrs);

            this.circle = circle;

        },

        circleAttrs : function(circles) {
            circles
                .attr("r", ƒ('radius'));
        },


        //
        //
        //
        getCirclePositions: function(){
            var that = this
            var circlePositions = {};
            this.circle.each(function(d){

                circlePositions[d.id] = {
                    x:Math.round(d.x),
                    y:Math.round(d.y)
                }


            })
                return JSON.stringify(circlePositions)
        },



        //
        //
        //
        start: function() {
            var that = this;
            this.force = d3.layout.force()
                .nodes(this.nodes)
                .size([this.width, this.height]);
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
                    that.circle
                        .each(that.totalSort(e.alpha))
                            .each(that.buoyancy(e.alpha))
                                .attr("cx", function(d) { return d.x; })
                        .attr("cy", function(d) { return d.y; });
                })
                .start();

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


                if (d.isNegative) {
                    if (d.changeCategory > 0) {
                        d.x = - 200
                    } else {
                        d.x =  1100
                    }
                }

                // if (d.positions.total) {
                //   targetX = d.positions.total.x
                //   targetY = d.positions.total.y
                // };



                //
                d.y = d.y + (targetY - d.y) * (that.defaultGravity + 0.02) * alpha
                d.x = d.x + (targetX - d.x) * (that.defaultGravity + 0.02) * alpha

            };
        },

        //
        //
        //
        buoyancy: function(alpha) {
            var that = this;
            return function(d){
                // d.y -= 1000 * alpha * alpha * alpha * d.changeCategory

                // if (d.changeCategory >= 0) {
                //   d.y -= 1000 * alpha * alpha * alpha
                // } else {
                //   d.y += 1000 * alpha * alpha * alpha
                // }

                var targetY = that.centerY;
                d.y = d.y + (targetY - d.y) * (that.defaultGravity) * alpha * alpha * alpha * 100



            };
        },


        //
        //
        //
        comparisonSort: function(alpha) {
            var that = this;
            return function(d){
                var targetY = that.height / 2;
                var targetX = 650;


                d.y = d.y + (targetY - d.y) * (that.defaultGravity) * alpha
                d.x = d.x + (targetX - d.x) * (that.defaultGravity) * alpha
            };
        },



        //
        //
        //
        collide: function(alpha){
            var that = this;
            var padding = 6;
            var quadtree = d3.geom.quadtree(this.nodes);
            return function(d) {
                var r = d.radius + that.maxRadius + padding,
                nx1 = d.x - r,
                nx2 = d.x + r,
                ny1 = d.y - r,
                ny2 = d.y + r;
                quadtree.visit(function(quad, x1, y1, x2, y2) {
                    if (quad.point && (quad.point !== d) && (d.group === quad.point.group)) {
                        var x = d.x - quad.point.x,
                        y = d.y - quad.point.y,
                        l = Math.sqrt(x * x + y * y),
                        r = d.radius + quad.point.radius;
                        if (l < r) {
                            l = (l - r) / l * alpha;
                            d.x -= x *= l;
                            d.y -= y *= l;
                            quad.point.x += x;
                            quad.point.y += y;
                        }
                    }
                    return x1 > nx2
                        || x2 < nx1
                        || y1 > ny2
                        || y2 < ny1;
                });
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
        }
    }
};

g.ready = function() {
    g.c = new g.Chart();
    g.c.init();
}

if (!!document.createElementNS && !!document.createElementNS('http://www.w3.org/2000/svg', "svg").createSVGRect){
    $(document).ready($.proxy(g.ready, this));
} else {
    $("#container").hide();
}
