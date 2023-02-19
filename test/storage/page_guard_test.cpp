//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

TEST(PageGuardTest, MultiPageMoveTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t pid2;
  page_id_t pid3;
  auto *p2 = bpm->NewPage(&pid2);
  auto *p3 = bpm->NewPage(&pid3);
  {
    auto gp2 = BasicPageGuard(bpm.get(), p2);
    auto gp3 = BasicPageGuard(bpm.get(), p3);
    gp3 = std::move(gp2);
    EXPECT_EQ(0, p3->GetPinCount());
    EXPECT_EQ(1, p2->GetPinCount());
    gp2.Drop();
    EXPECT_EQ(1, p2->GetPinCount());
  }
  EXPECT_EQ(0, p2->GetPinCount());
  disk_manager->ShutDown();
}

// NOLINTNEXTLINE
TEST(PageGuardTest, SinglePageMoveTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);
  EXPECT_EQ(1, page0->GetPinCount());

  auto p1 = bpm->FetchPageBasic(page_id_temp);
  EXPECT_EQ(2, page0->GetPinCount());
  auto p2 = std::move(p1);
  EXPECT_EQ(2, page0->GetPinCount());
  auto p3 = bpm->FetchPageBasic(page_id_temp);
  EXPECT_EQ(3, page0->GetPinCount());

  p3 = std::move(p2);
  EXPECT_EQ(2, page0->GetPinCount());
  EXPECT_EQ(page0->GetData(), p3.GetData());
  p3.Drop();
  EXPECT_EQ(1, page0->GetPinCount());
  p3.Drop();
  EXPECT_EQ(1, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);
  EXPECT_EQ(1, page0->GetPinCount());

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
