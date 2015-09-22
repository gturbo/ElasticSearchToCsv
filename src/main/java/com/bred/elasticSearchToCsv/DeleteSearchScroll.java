package com.bred.elasticSearchToCsv;

import io.searchbox.action.AbstractMultiIndexActionBuilder;
import io.searchbox.action.GenericResultAbstractAction;
import io.searchbox.params.Parameters;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * @author gael urbauer
 * copied from source class io.searchbox.core.SearchScroll
 */
public class DeleteSearchScroll extends GenericResultAbstractAction {
    static final int MAX_SCROLL_ID_LENGTH = 1900;

    protected DeleteSearchScroll(Builder builder) {
        super(builder);
        setURI(buildURI());
        payload = builder.getScrollId();
    }

    @Override
    protected String buildURI() {
        return super.buildURI() + "/_search/scroll";
    }

    @Override
    public String getRestMethodName() {
        return "DELETE";
    }

    @Override
    public String getPathToResult() {
        return "hits/hits/_source";
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }

        DeleteSearchScroll rhs = (DeleteSearchScroll) obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .isEquals();
    }

    public static class Builder extends AbstractMultiIndexActionBuilder<DeleteSearchScroll, Builder> {

        private final String scrollId;

        public Builder(String scrollId) {
            this.scrollId = scrollId;
            if (scrollId.length() <= MAX_SCROLL_ID_LENGTH) {
                setParameter(Parameters.SCROLL_ID, scrollId);
            }
        }

        @Override
        public String getJoinedIndices() {
            if (indexNames.size() > 0) {
                return StringUtils.join(indexNames, ",");
            } else {
                return null;
            }
        }

        @Override
        public DeleteSearchScroll build() {
            return new DeleteSearchScroll(this);
        }

        public String getScrollId() {
            return scrollId;
        }
    }
}
