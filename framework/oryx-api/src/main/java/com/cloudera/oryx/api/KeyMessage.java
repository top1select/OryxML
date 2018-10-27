
package com.cloudera.oryx.api;

import java.io.Serializable;


public interface KeyMessage<K, M> extends Serializable {
    K getKey();

    M getMessage();
}
