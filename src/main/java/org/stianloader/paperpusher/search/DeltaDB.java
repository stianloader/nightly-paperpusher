package org.stianloader.paperpusher.search;

import java.util.List;

import javax.annotation.Nonnegative;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

class DeltaDB {

    enum ChangeType {
        ADDED("added"),
        CONTENTS_CHANGED("touched"),
        REMOVED("removed");

        private static final @NotNull ChangeType @NotNull[] INTERNAL_LOOKUP = ChangeType.values();

        @Contract(pure = true)
        public static final int getValueCount() {
            return ChangeType.INTERNAL_LOOKUP.length;
        }

        @NotNull
        @Contract(pure = true)
        public static final ChangeType lookupValue(@Nonnegative int ordinal) {
            return ChangeType.INTERNAL_LOOKUP[ordinal];
        }

        @NotNull
        private final String verb;

        private ChangeType(@NotNull String verb) {
            this.verb = verb;
        }

        @NotNull
        @Contract(pure = true)
        public String getVerb() {
            return this.verb;
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

    @NotNull
    static final String DEFAULT_PACKAGE_NAME = "(default package)";

    @NotNull
    static final String SQL_PREPARED_INSERT_CLASSDELTA = "INSERT OR FAIL INTO classdelta (rowid, classId, versionId, changetype) VALUES (?, ?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_CLASSDELTA_PARTIAL = "INSERT OR FAIL INTO classdelta (classId, versionId, changetype) VALUES (?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_CLASSID = "INSERT OR FAIL INTO classid (rowid, packageId, className) VALUES (?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_CLASSID_PARTIAL = "INSERT OR FAIL INTO classid (packageId, className) VALUES (?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_GAID = "INSERT OR FAIL INTO gaid (rowid, groupId, artifactId) VALUES (?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_GAID_PARTIAL = "INSERT OR FAIL INTO gaid (groupId, artifactId) VALUES (?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_GAVID = "INSERT OR FAIL INTO gavid (rowid, gaId, version) VALUES (?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_GAVID_PARTIAL = "INSERT OR FAIL INTO gavid (gaId, version) VALUES (?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_MEMBERDELTA = "INSERT OR FAIL INTO memberdelta (rowid, memberId, versionId, changetype) VALUES (?, ?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_MEMBERDELTA_PARTIAL = "INSERT OR FAIL INTO memberdelta (memberId, versionId, changetype) VALUES (?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_MEMBERID = "INSERT OR FAIL INTO memberid (rowid, classId, memberName, memberDesc) VALUES (?, ?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_MEMBERID_PARTIAL = "INSERT OR FAIL INTO memberid (classId, memberName, memberDesc) VALUES (?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_PACKAGEDELTA = "INSERT OR FAIL INTO packagedelta (rowid, packageId, versionId, changetype) VALUES (?, ?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_PACKAGEDELTA_PARTIAL = "INSERT OR FAIL INTO packagedelta (packageId, versionId, changetype) VALUES (?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_PACKAGEID = "INSERT OR FAIL INTO packageid (rowid, gaId, packageName) VALUES (?, ?, ?)";

    @NotNull
    static final String SQL_PREPARED_INSERT_PACKAGEID_PARTIAL = "INSERT OR FAIL INTO packageid (gaId, packageName) VALUES (?, ?)";

    @NotNull
    static final String SQL_PREPARED_LOOKUP_CLASS_DELTAS_FULL = "SELECT changetype, packageName, className FROM classdelta, classid, packageid WHERE versionId = ? AND classdelta.classId = classid.rowid AND classid.packageId = packageid.rowid";

    @NotNull
    static final String SQL_PREPARED_LOOKUP_CLASSID = "SELECT rowid FROM classid WHERE packageId = ? AND className = ?";

    @NotNull
    static final String SQL_PREPARED_LOOKUP_GAVID = "SELECT rowid FROM gavid WHERE gaId = ? AND version = ?";

    @NotNull
    static final String SQL_PREPARED_LOOKUP_MEMBER_DELTAS_FULL = "SELECT changetype, packageName, className, memberName, memberDesc FROM memberdelta, memberid, classid, packageid WHERE versionId = ? AND memberdelta.memberId = memberid.rowid AND memberid.classId = classid.rowid AND classid.packageId = packageid.rowid";

    @NotNull
    static final String SQL_PREPARED_LOOKUP_MEMBERID = "SELECT rowid FROM memberid WHERE classId = ? AND memberName = ? AND memberDesc = ?";

    @NotNull
    static final String SQL_PREPARED_LOOKUP_PACKAGE_DELTAS_FULL = "SELECT changetype, packageName FROM packagedelta, packageid WHERE versionId = ? AND packagedelta.packageid = packageid.rowid";

    @NotNull
    static final String SQL_PREPARED_LOOKUP_PACKAGEID = "SELECT rowid FROM packageid WHERE gaId = ? AND packageName = ?";

    @NotNull
    @Contract(pure = true)
    static final String getClassShortName(@NotNull String classFQN) {
        int slashIndex = classFQN.lastIndexOf('/');
        if (slashIndex < 0) {
            return classFQN;
        } else {
            return classFQN.substring(slashIndex + 1);
        }
    }

    @NotNull
    @Contract(pure = true)
    static final String getPackageName(@NotNull String classFQN) {
        int slashIndex = classFQN.lastIndexOf('/');
        if (slashIndex < 0) {
            return DeltaDB.DEFAULT_PACKAGE_NAME;
        } else {
            return classFQN.substring(0, slashIndex);
        }
    }
}
