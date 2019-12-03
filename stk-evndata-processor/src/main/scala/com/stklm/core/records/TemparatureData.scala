package com.stklm.core.records

case class TemparatureData (year:Int,month: Int,day:Int, tMorn: BigDecimal, 
                            tNoon: BigDecimal, tEvn: BigDecimal, tMax: BigDecimal, 
                            tMin: BigDecimal, tMean: BigDecimal)