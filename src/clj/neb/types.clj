(ns neb.types
  (:require [taoensso.nippy :as nippy])
  (:import [org.shisoft.neb.io writer reader type_lengths]
           (java.util UUID Date)
           (java.nio.charset Charset)))

(declare read-array)
(declare write-array)

(defmacro defDataTypes [n m]
  `(def ~n
     ~(into
        {}
        (map
          (fn [[k {:keys [id example dynamic? preproc succproc dep] :as v}]]
            (let [obj-symbol (symbol "obj")
                  length (when-not dynamic? (symbol (str "type_lengths/" (name k) "Len")))]
              [k (merge v
                        {:reader (when-not dep
                                   `(fn [trunk# offset#]
                                      (~(symbol (str "reader/read"
                                                     (clojure.string/capitalize (name k))))
                                        trunk#
                                        offset#)))
                         :writer (when-not dep
                                   `(fn [trunk# value# offset#]
                                      (~(symbol (str "writer/write"
                                                     (clojure.string/capitalize (name k))))
                                        trunk#
                                        value#
                                        offset#)))
                         :length length
                         :unit-length (when dynamic? (symbol (str "type_lengths/" (name k) "UnitLen")))
                         :count-length (when dynamic?
                                         `(fn [~obj-symbol]
                                            (~(symbol (str "type_lengths/count"
                                                           (clojure.string/capitalize (name k))))
                                              ~obj-symbol)))})]))
          m))))

(def ^Charset string-encoding (Charset/forName "UTF-8"))

(defDataTypes data-types
  {
   :char    {:id      1
             :example [\a \测 \å \∫ \≤ \œ]}
   :text    {:id      3 :dynamic? true
             :example ["The morpueus engine" "这是一段测试文本 abc"]
             :encoder (fn [^String string] (.getBytes string string-encoding))
             :decoder (fn [byte-arr] (String. byte-arr string-encoding))
             :dep :bytes}
   :int     {:id      4
             :example [`(int 1) `(int Integer/MIN_VALUE) `(int Integer/MAX_VALUE)]}
   :long    {:id      5
             :example [1 2 `Long/MIN_VALUE `Long/MAX_VALUE]}
   :boolean {:id      6
             :example [true false]}
   :short   {:id      7
             :example [`(short 1) `(short 2) `Short/MIN_VALUE `Short/MAX_VALUE]}
   :ushort  {:id      8
             :example [`(int 1) `(int 2) 0 `(int (* 2 Short/MAX_VALUE))]}
   :byte    {:id      9
             :example [`(byte 1) `(byte 2) `(byte (Byte/MIN_VALUE)) `(byte (Byte/MAX_VALUE))]}
   :bytes   {:id      10 :dynamic? true
             :example [`(.getBytes "this is a bytes test")]}
   :float   {:id      11
             :example [`(float 1) `(float 2) `Float/MIN_VALUE `Float/MAX_VALUE]}
   :double  {:id      12
             :example [`(double 1.0) `(double 2.0) `Double/MIN_VALUE `Double/MAX_VALUE]}
   :uuid    {:id      13
             :example [`(UUID/randomUUID) `(UUID/randomUUID)]}
   :cid     {:id      14
             :example [`(UUID/randomUUID) `(UUID/randomUUID)]}
   :pos2d   {:id      15
             :example [[1.0, 2.0] [3.0 4.0]]
             :encoder double-array
             :decoder vec}
   :pos3d   {:id      16
             :example [[1.0, 2.0 3.0] [3.0 4.0 5.0]]
             :encoder double-array
             :decoder vec}
   :pos4d   {:id      17
             :example [[1.0, 2.0 3.0 1024.0] [3.0 4.0 4096.0 12.0]]
             :encoder double-array
             :decoder vec}
   :geo     {:id      18
             :example [[31.12 121.30] [40.30 71.51]]
             :encoder float-array
             :decoder vec}
   :date    {:id      19
             :example [`(Date.)]}
   :obj     {:id      20 :dynamic? true
             :encoder nippy/freeze
             :decoder nippy/thaw
             :dep :bytes
             :example [{:a 1 :b 2}]}
   })