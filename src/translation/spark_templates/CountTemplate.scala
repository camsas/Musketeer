val aint_{{OUTPUT}} = {{CONDITION}}.count()
val ares_{{OUTPUT}} = Seq(("ALL",  aint_{{OUTPUT}}.toInt))
{{OUTPUT}} = sc.parallelize((ares_{{OUTPUT}})){{TO_CACHE}}

