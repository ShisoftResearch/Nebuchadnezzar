(ns neb.durability
  (:import (org.shisoft.neb Trunk)))

;TODO: Durability for Nebuchadnezzar is still a undetermined feature.
;      The ideal design is to provide multi-master replication backend. Right now, there will be no replication.
;      Each object will only have on copy on local hard drive.
;      I thought about using memory map file, but it is uncontrolable and is also impossible to impelement replication in the future

(def data-path (atom nil))

(defn enable-durability [^Trunk trunk path]
  (reset! data-path path))