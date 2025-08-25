package org.stianloader.paperpusher.search;

import java.util.List;

import org.jetbrains.annotations.NotNull;

class DeltaDB {

    @NotNull
    static final String DEFAULT_PACKAGE_NAME = "(default package)";

    enum ChangeType {
        ADDED,
        REMOVED,
        CONTENTS_CHANGED;

        private static final @NotNull ChangeType @NotNull[] INTERNAL_LOOKUP = ChangeType.values();

        @NotNull
        public static final ChangeType lookupValue(int ordinal) {
            return ChangeType.INTERNAL_LOOKUP[ordinal];
        }
    }

    record ProtoGAId(int rowId, @NotNull String groupId, @NotNull String artifactId) {}

    record ProtoGAVId(int rowId, int gaId, @NotNull String version) {}

    record ProtoPackageId(int rowId, int gaId, @NotNull String packageName) {}

    record ProtoPackageDelta(int rowId, int packageId, int versionId, int changeType) {}

    record ProtoClassId(int rowId, int packageId, @NotNull String className) {}

    record ProtoClassDelta(int rowId, int classId, int versionId, int changeType) {}

    record ProtoMemberId(int rowId, int classId, @NotNull String memberName, @NotNull String memberDesc) {}

    record ProtoMemberDelta(int rowId, int memberId, int versionId, int changeType) {}

    record ProtoDatabase(
        @NotNull List<@NotNull ProtoGAId> gaid,
        @NotNull List<@NotNull ProtoGAVId> gavid,
        @NotNull List<@NotNull ProtoPackageId> packageid,
        @NotNull List<@NotNull ProtoPackageDelta> packagedelta,
        @NotNull List<@NotNull ProtoClassId> classid,
        @NotNull List<@NotNull ProtoClassDelta> classdelta,
        @NotNull List<@NotNull ProtoMemberId> memberid,
        @NotNull List<@NotNull ProtoMemberDelta> memberdelta
    ) {}

}
